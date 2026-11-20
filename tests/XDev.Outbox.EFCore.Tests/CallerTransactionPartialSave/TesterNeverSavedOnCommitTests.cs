using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Adversarial (tester) checks of fix-commit-drops-never-saved-events: a commit of a caller transaction keeps
/// buffered events that were never saved in it. Looks for duplicate rows (an event committed in a transaction saved
/// again later) and phantom dispatches across several saves, savepoints, D-C, AcceptAllChanges, commit failures
/// before and after the server commit, EF-internal transactions (EnsureCreated, ExecuteDelete), a retrying strategy
/// and events added during a save.
/// </summary>
public class TesterNeverSavedOnCommitTests
{
    public static TheoryData<bool> Async() => new() { false, true };

    public static TheoryData<bool, bool> AsyncFlag()
    {
        var data = new TheoryData<bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(async, flag);
        return data;
    }

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static User AddUserWithEvent(AdvDbContext db, string email, out UserCreated evt)
    {
        var user = new User { Email = email };
        db.Users.Add(user);
        evt = new UserCreated { User = user };
        db.Outbox.AddEvent(evt);
        return user;
    }

    /// <summary>Committed users are exactly <paramref name="emails"/>, one outbox row each carrying its user's key, each row dispatched exactly once.</summary>
    private static void AssertCommittedWithOneRowEach(CommitTesterHost host, params string[] emails)
    {
        var users = host.Users();
        var rows = host.UserRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        Assert.Equal(
            users.OrderBy(u => u.Email).Select(u => (u.Id, u.Email)),
            rows.Select(r => Payload.User(r.Payload)).Select(p => (p.GetProperty("id").GetInt32(), p.GetProperty("email").GetString()!)).OrderBy(x => x.Item2));
        Assert.Equal(host.OutboxRows().Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id).OrderBy(x => x));
    }

    /// <summary>A later save of an unrelated user (and a later empty transaction) writes and dispatches nothing more.</summary>
    private static async Task AssertNothingMoreIsWritten(CommitTesterHost host, AdvDbContext db, bool async)
    {
        var rows = host.OutboxRows().Count;
        var dispatched = host.Queue.Enqueued.Count;
        using (var tx = await Begin(db, async))
        {
            await Commit(tx, async);
        }
        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "unrelated" });
        await Save.Invoke(db, async);
        Assert.Equal(rows, host.OutboxRows().Count);
        Assert.Equal(dispatched, host.Queue.Enqueued.Count);
        Assert.Empty(db.Outbox.GetEvents());
    }

    // ---------------- Committed events are never saved again ----------------

    /// <summary>
    /// Two accepting saves with events in one transaction and a third event added after the last save. The commit
    /// dispatches the two saved events once and keeps only the third; the next save writes only the third; nothing is
    /// rewritten afterwards.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task TwoSavesAndEventAddedAfterLastSave_Commit_SavedEventsCommittedOnceThirdKeptAndWrittenOnce(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        UserCreated third;
        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "first", out _);
            await Save.Invoke(db, async, true);
            AddUserWithEvent(db, "second", out _);
            await Save.Invoke(db, async, true);
            AddUserWithEvent(db, "third", out third);
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "first", "second");
        Assert.Same(third, Assert.Single(db.Outbox.GetEvents()));

        await Save.Invoke(db, async);

        AssertCommittedWithOneRowEach(host, "first", "second", "third");
        await AssertNothingMoreIsWritten(host, db, async);
        AssertCommittedWithOneRowEach(host, "first", "second", "third");
    }

    /// <summary>
    /// D-C: a flag = false save of a modification with its event, then an accepting save without events (re-runs the
    /// idempotent UPDATE and accepts everything), then a commit: the saved event leaves the buffer and is never
    /// written again.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task FlagFalseSaveThenAcceptingSaveWithoutEvents_Commit_EventCommittedOnceAndNotRewritten(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        await Save.Invoke(db, async);

        using (var tx = await Begin(db, async))
        {
            user.Email = "b";
            db.Outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(db, async, false);
            db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "dc" });
            await Save.Invoke(db, async, true);
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "b");
        Assert.Empty(db.Outbox.GetEvents());
        await AssertNothingMoreIsWritten(host, db, async);
        AssertCommittedWithOneRowEach(host, "b");
    }

    /// <summary>
    /// The EF retry pattern: SaveChanges(false), ChangeTracker.AcceptAllChanges() (not observed by the library), then
    /// the commit. The saved event leaves the buffer at commit and is never written again.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task FlagFalseSaveThenDirectAcceptAllChanges_Commit_EventCommittedOnceAndNotRewritten(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "a", out _);
            await Save.Invoke(db, async, false);
            db.ChangeTracker.AcceptAllChanges();
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "a");
        Assert.Empty(db.Outbox.GetEvents());
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// A user savepoint undoes the second save (flag = false: its event is kept with its still-Added user); the
    /// caller saves again and commits. Each event committed exactly once, none kept, nothing rewritten later.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task RollbackToUserSavepointThenReSaveThenCommit_EachEventCommittedOnce(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "first", out _);
            await Save.Invoke(db, async, true);
            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            var second = AddUserWithEvent(db, "second", out var secondEvent);
            await Save.Invoke(db, async, false);
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");

            Assert.Equal(EntityState.Added, db.Entry(second).State);
            Assert.Contains(secondEvent, db.Outbox.GetEvents());

            await Save.Invoke(db, async, true);
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "first", "second");
        Assert.Empty(db.Outbox.GetEvents());
        await AssertNothingMoreIsWritten(host, db, async);
        AssertCommittedWithOneRowEach(host, "first", "second");
    }

    /// <summary>
    /// A user savepoint undoes the second save and the caller commits without saving again: the first event is
    /// committed once; the undone event follows B1 (flag = false: kept with its Added user and written once by the next
    /// save; flag = true: its user was accepted, so it is dropped). No duplicate of the first event.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task RollbackToUserSavepointThenCommitWithoutReSave_UndoneEventFollowsB1_FirstNotRewritten(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        UserCreated secondEvent;
        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "first", out _);
            await Save.Invoke(db, async, true);
            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            AddUserWithEvent(db, "second", out secondEvent);
            await Save.Invoke(db, async, flag);
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "first");
        if (flag)
        {
            Assert.Empty(db.Outbox.GetEvents());
            await AssertNothingMoreIsWritten(host, db, async);
            AssertCommittedWithOneRowEach(host, "first");
        }
        else
        {
            Assert.Same(secondEvent, Assert.Single(db.Outbox.GetEvents()));
            await Save.Invoke(db, async);
            AssertCommittedWithOneRowEach(host, "first", "second");
            await AssertNothingMoreIsWritten(host, db, async);
        }
    }

    /// <summary>
    /// The same event instance added twice, saved once (both copies become pending), then added a third time after
    /// the save: the commit commits two rows and keeps exactly one copy (multiplicity), written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task SameInstanceAddedTwiceSavedThenAddedAgain_Commit_KeepsExactlyTheUnsavedCopy(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var evt = new PlainEvent { Text = "x" };

        using (var tx = await Begin(db, async))
        {
            db.Outbox.AddEvent(evt);
            db.Outbox.AddEvent(evt);
            db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "d" });
            await Save.Invoke(db, async, true);
            db.Outbox.AddEvent(evt);
            await Commit(tx, async);
        }

        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(2, host.Queue.Enqueued.Count);
        Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));

        await Save.Invoke(db, async);
        Assert.Equal(3, host.OutboxRows().Count);
        Assert.Equal(3, host.Queue.Enqueued.Count);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Commit failures ----------------

    /// <summary>
    /// The commit fails before reaching the server (TransactionCommitting throws): a failed commit is an end without
    /// commit. Nothing is committed or dispatched; the saved event follows B1 and the never-saved event is kept; a new
    /// transaction writes what remains exactly once.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task CommitFailsBeforeServer_NothingCommitted_RetryWritesEachEventOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        User saved;
        using (var tx = await Begin(db, async))
        {
            saved = AddUserWithEvent(db, "saved", out _);
            await Save.Invoke(db, async, flag);
            AddUserWithEvent(db, "never", out _);
            host.Faults.FailCommits();
            var error = await Record.ExceptionAsync(() => Commit(tx, async));
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(error), error?.ToString());
        }

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        // B1: the saved event stays only when its changes are still pending (flag = false).
        Assert.Equal(flag ? 1 : 2, db.Outbox.GetEvents().Count);
        // flag = true accepted the saved user although its insert was rolled back (plain EF): it is not saved again.
        Assert.Equal(flag ? EntityState.Unchanged : EntityState.Added, db.Entry(saved).State);
        // Plain EF: that ghost keeps a rolled-back key SQLite reuses for the next insert (identity conflict), so the
        // application detaches it.
        if (flag) db.Entry(saved).State = EntityState.Detached;

        using (var tx = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        if (flag) AssertCommittedWithOneRowEach(host, "never");
        else AssertCommittedWithOneRowEach(host, "saved", "never");
        Assert.Equal(flag ? 1 : 2, host.Users().Count);
    }

    /// <summary>
    /// CHARACTERIZATION (commit-unknown, not changed by this fix). The server commits, then an application interceptor
    /// registered before the library's throws in TransactionCommitted. EF reports a failed "Commit" and the library's
    /// TransactionCommitted is not called, so the library treats it as an end without commit: no in-memory dispatch
    /// (the committed row is left to the poller); the saved event follows B1 (flag = false: kept with its still-Modified
    /// user, so the next save writes the event AGAIN, a duplicate row as with plain EF's re-run UPDATE; flag = true:
    /// dropped); the never-saved event is kept and written once. Same outcome before the fix: this path never reaches
    /// the library's TransactionCommitted.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task ServerCommittedButAppInterceptorThrowsAfterCommit_TreatedAsUncommitted_SavedEventFollowsB1(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        await Save.Invoke(db, async);

        var savedEvent = new UserCreated { User = user };
        var neverEvent = new PlainEvent { Text = "never" };
        using (var tx = await Begin(db, async))
        {
            user.Email = "changed";
            db.Outbox.AddEvent(savedEvent);
            await Save.Invoke(db, async, flag);
            db.Outbox.AddEvent(neverEvent);
            host.AfterCommit.ThrowOnce = true;
            var error = await Record.ExceptionAsync(() => Commit(tx, async));
            Assert.IsType<AfterCommitSabotageException>(error);
        }

        // Committed on the server, not dispatched in memory.
        Assert.Equal(new[] { "changed" }, host.Users().Select(u => u.Email));
        Assert.Single(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);

        if (flag) Assert.Equal(new object[] { neverEvent }, db.Outbox.GetEvents());
        else Assert.Equal(new object[] { savedEvent, neverEvent }, db.Outbox.GetEvents());

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "next" });
        await Save.Invoke(db, async);

        var rows = host.OutboxRows();
        Assert.Single(rows, r => r.Payload.Contains("\"never\""));
        Assert.Equal(flag ? 1 : 2, rows.Count(r => r.Payload.Contains("\"user\"")));
        Assert.Equal(flag ? 1 : 2, host.Queue.Enqueued.Count);
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// The server commits and the library's TransactionCommitted runs, then the in-memory dispatch throws (queue
    /// failure). EF reports a failed "Commit", but the library had already removed the committed event, so nothing
    /// is released or saved again; the never-saved event is kept and written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task ServerCommittedThenDispatchThrows_CommittedEventNotRewritten_NeverSavedWrittenOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        User saved;
        UserCreated neverEvent;
        using (var tx = await Begin(db, async))
        {
            saved = AddUserWithEvent(db, "saved", out _);
            await Save.Invoke(db, async, flag);
            AddUserWithEvent(db, "never", out neverEvent);
            host.Queue.FailNext = true;
            var error = await Record.ExceptionAsync(() => Commit(tx, async));
            Assert.True(ExceptionChain.Contains<QueueFailureException>(error), error?.ToString());
        }

        Assert.Equal(new[] { "saved" }, host.Users().Select(u => u.Email));
        Assert.Single(host.OutboxRows());
        Assert.Same(neverEvent, Assert.Single(db.Outbox.GetEvents()));

        if (!flag) db.Entry(saved).State = EntityState.Unchanged;
        await Save.Invoke(db, async);

        Assert.Equal(new[] { "never", "saved" }, host.Users().Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(2, host.OutboxRows().Count);
        // Only the never-saved event's row was dispatched in memory; the first is left to the poller.
        var dispatched = Assert.Single(host.Queue.Enqueued);
        Assert.Equal("never", Payload.User(host.OutboxRows().Single(r => r.Id == dispatched.Id).Payload).GetProperty("email").GetString());
        Assert.Empty(db.Outbox.GetEvents());
    }

    // ---------------- EF-internal transactions and set-based updates ----------------

    /// <summary>
    /// EnsureCreated runs its DDL in a transaction EF begins on this context's connection, so the library's
    /// interceptor sees a commit of a transaction with no pending message. A buffered event is not dropped or
    /// dispatched by it; the next save writes it once. (Before the fix the internal commit dropped it.)
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task EfInternalTransactionCommit_EnsureCreated_BufferedEventKeptAndNotDispatched(bool async)
    {
        using var host = CommitTesterHost.Create(ensureCreated: false);
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        AddUserWithEvent(db, "a", out var evt);
        host.Commits.Reset();
        if (async) await db.Database.EnsureCreatedAsync(); else db.Database.EnsureCreated();

        Assert.True(host.Commits.Count > 0, "EnsureCreated is expected to commit a transaction on the context.");
        Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));
        Assert.Empty(host.Queue.Enqueued);

        await Save.Invoke(db, async);
        AssertCommittedWithOneRowEach(host, "a");
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// CONSUMER HAZARD (behavior of the approved fix). The application records an event for a set-based change
    /// (ExecuteDelete) in its transaction and commits without SaveChanges. The event is kept (before the fix the commit
    /// dropped it, so it was lost); it is written and dispatched only by the next, unrelated save of the context, in a
    /// different transaction than the change it describes, or never if the context ends first.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task EventForExecuteDeleteThenCommitWithoutSave_EventKept_WrittenByNextUnrelatedSave(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "old" });
        await Save.Invoke(db, async);
        db.ChangeTracker.Clear();

        var evt = new PlainEvent { Text = "docs deleted" };
        using (var tx = await Begin(db, async))
        {
            if (async) await db.Docs.ExecuteDeleteAsync(); else db.Docs.ExecuteDelete();
            db.Outbox.AddEvent(evt);
            await Commit(tx, async);
        }

        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Docs\""));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));

        db.Users.Add(new User { Email = "unrelated" });
        await Save.Invoke(db, async);

        var row = Assert.Single(host.OutboxRows());
        Assert.Contains("docs deleted", row.Payload);
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
    }

    /// <summary>
    /// CONSUMER HAZARD (behavior of the approved fix): a unit of work that records an event without SaveChanges, run in
    /// a retrying strategy whose first commit fails, adds the event on each attempt; both copies are kept (the first by
    /// the failed commit, since fix-rollback-event-loss; the second by the successful commit, since this fix) and the
    /// next save writes two rows for one change. Before this fix the successful commit dropped both (event lost).
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task EventWithoutSaveInRetriedUnitOfWork_BothAttemptsKept_NextSaveWritesTwoRows(bool async)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        host.Faults.FailCommits();
        var strategy = db.Database.CreateExecutionStrategy();
        if (async)
        {
            await strategy.ExecuteAsync(async () =>
            {
                await using var tx = await db.Database.BeginTransactionAsync();
                await db.Database.ExecuteSqlRawAsync("DELETE FROM \"Docs\"");
                db.Outbox.AddEvent(new PlainEvent { Text = "attempt" });
                await tx.CommitAsync();
            });
        }
        else
        {
            strategy.Execute(() =>
            {
                using var tx = db.Database.BeginTransaction();
                db.Database.ExecuteSqlRaw("DELETE FROM \"Docs\"");
                db.Outbox.AddEvent(new PlainEvent { Text = "attempt" });
                tx.Commit();
            });
        }

        Assert.Equal(1, host.Faults.CommitFailuresFired);
        Assert.Equal(2, db.Outbox.GetEvents().Count);

        db.Users.Add(new User { Email = "unrelated" });
        await Save.Invoke(db, async);
        Assert.Equal(2, host.OutboxRows().Count);
    }

    /// <summary>
    /// The recommended retry pattern (BeginTransaction, SaveChanges(false), Commit inside the strategy, then
    /// AcceptAllChanges) with a commit failing before the server once: the user and its event are committed once and
    /// dispatched once; nothing is kept or rewritten.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task RetryingStrategyUserTransaction_CommitFailsOnce_CommittedAndDispatchedOnce(bool async)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        AddUserWithEvent(db, "a", out _);

        host.Faults.FailCommits();
        var strategy = db.Database.CreateExecutionStrategy();
        if (async)
        {
            await strategy.ExecuteAsync(async () =>
            {
                await using var tx = await db.Database.BeginTransactionAsync();
                await db.SaveChangesAsync(false);
                await tx.CommitAsync();
            });
        }
        else
        {
            strategy.Execute(() =>
            {
                using var tx = db.Database.BeginTransaction();
                db.SaveChanges(false);
                tx.Commit();
            });
        }
        db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(1, host.Faults.CommitFailuresFired);
        AssertCommittedWithOneRowEach(host, "a");
        Assert.Empty(db.Outbox.GetEvents());
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Events added during a save ----------------

    /// <summary>
    /// KNOWN GAP (characterization, found by the developer; not changed by this fix). An event added by a SavingChanges
    /// handler during an owned save (no caller transaction) is dropped without a row: CompleteOwnedSave clears the
    /// whole buffer. Its business change (none here) would be committed without an event.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task KnownGap_EventAddedBySavingChangesDuringOwnedSave_DroppedWithoutRow(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" });
        };

        AddUserWithEvent(db, "a", out _);
        await Save.Invoke(db, async);

        AssertCommittedWithOneRowEach(host, "a");
        Assert.DoesNotContain(host.OutboxRows(), r => r.Payload.Contains("during save"));
        // Dropped: never written by this or any later save.
        Assert.Empty(db.Outbox.GetEvents());
        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);
        Assert.Single(host.OutboxRows());
    }

    /// <summary>
    /// The same handler during a save in a caller transaction: the event is not part of that save, stays buffered
    /// across the commit (since this fix; before, the commit dropped it), and is written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task EventAddedBySavingChangesDuringCallerSave_KeptAtCommit_WrittenOnceByNextSave(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        PlainEvent? during = null;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(during = new PlainEvent { Text = "during save" });
        };

        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "a", out _);
            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "a");
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);
        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Single(host.OutboxRows(), r => r.Payload.Contains("during save"));
        Assert.Equal(2, host.Queue.Enqueued.Count);
        Assert.Empty(db.Outbox.GetEvents());
    }

    // ---------------- Owned save and pooled lease after a kept event ----------------

    /// <summary>
    /// An event kept by a commit is written once by a later owned save (no caller transaction) together with a new
    /// event; each dispatched once; nothing is kept or rewritten.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task EventKeptByCommit_OwnedSaveWithAnotherEvent_EachWrittenAndDispatchedOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        AddUserWithEvent(db, "kept", out _);
        using (var tx = await Begin(db, async))
        {
            await Commit(tx, async);
        }
        AddUserWithEvent(db, "new", out _);
        await Save.Invoke(db, async, flag);
        if (!flag) db.ChangeTracker.AcceptAllChanges();

        AssertCommittedWithOneRowEach(host, "kept", "new");
        Assert.Empty(db.Outbox.GetEvents());
        await AssertNothingMoreIsWritten(host, db, async);
    }
}

public sealed class AfterCommitSabotageException : Exception
{
    public AfterCommitSabotageException() : base("Injected failure in TransactionCommitted (after the server commit)")
    {
    }
}

/// <summary>Throws once from TransactionCommitted, i.e. after the server committed.</summary>
public sealed class AfterCommitSabotage : DbTransactionInterceptor
{
    public bool ThrowOnce { get; set; }

    private void Fire()
    {
        if (!ThrowOnce) return;
        ThrowOnce = false;
        throw new AfterCommitSabotageException();
    }

    public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) => Fire();

    public override Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        Fire();
        return Task.CompletedTask;
    }
}

/// <summary>Counts commits seen on the context's connection.</summary>
public sealed class CommitCounter : DbTransactionInterceptor
{
    public int Count { get; private set; }

    public void Reset() => Count = 0;

    public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) => Count++;

    public override Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        Count++;
        return Task.CompletedTask;
    }
}

public sealed class QueueFailureException : Exception
{
    public QueueFailureException() : base("Injected message queue failure")
    {
    }
}

/// <summary>Records enqueued messages; <see cref="FailNext"/> makes the next enqueue throw without recording.</summary>
public sealed class FailingRecordingQueue : IMessageQueue<int>
{
    private readonly object _gate = new();
    private readonly List<OutboxMessage<int>> _items = new();

    public bool FailNext { get; set; }

    public IReadOnlyList<OutboxMessage<int>> Enqueued { get { lock (_gate) return _items.ToArray(); } }

    public Task EnqueueAsync(OutboxMessage<int> message, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (FailNext)
            {
                FailNext = false;
                throw new QueueFailureException();
            }
            _items.Add(message);
        }
        return Task.CompletedTask;
    }

    public event Func<OutboxMessage<int>, CancellationToken, Task>? MessageReceived { add { } remove { } }
    public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}

/// <summary>
/// SQLite in-memory host for <see cref="AdvDbContext"/> with fault injection, an after-commit sabotage interceptor
/// and a commit counter (all registered before the library's interceptor) and a queue that can fail.
/// </summary>
public sealed class CommitTesterHost : IDisposable
{
    private CommitTesterHost(SqliteConnection connection, ServiceProvider services, FailingRecordingQueue queue, FaultInjector faults, AfterCommitSabotage afterCommit, CommitCounter commits)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
        AfterCommit = afterCommit;
        Commits = commits;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public FailingRecordingQueue Queue { get; }
    public FaultInjector Faults { get; }
    public AfterCommitSabotage AfterCommit { get; }
    public CommitCounter Commits { get; }

    public static CommitTesterHost Create(bool retrying = false, bool ensureCreated = true)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new FailingRecordingQueue();
        var faults = new FaultInjector();
        var afterCommit = new AfterCommitSabotage();
        var commits = new CommitCounter();

        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddDbContext<AdvDbContext>(options =>
        {
            options.UseSqlite(connection, sqlite =>
            {
                if (retrying) sqlite.ExecutionStrategy(d => new TestRetryingExecutionStrategy(d));
            });
            options.AddInterceptors(new FaultCommandInterceptor(faults), new FaultCommitInterceptor(faults), afterCommit, commits);
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        });
        services.AddOutbox<int>(typeof(CommitTesterHost).Assembly);
        services.AddEFCoreOutboxStore<int, AdvDbContext>();

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        if (ensureCreated)
        {
            using var scope = provider.CreateScope();
            scope.ServiceProvider.GetRequiredService<AdvDbContext>().Database.EnsureCreated();
        }
        faults.ExecutedCommands.Clear();
        return new CommitTesterHost(connection, provider, queue, faults, afterCommit, commits);
    }

    public (IServiceScope Scope, AdvDbContext Db) CreateScope()
    {
        var scope = Services.CreateScope();
        return (scope, scope.ServiceProvider.GetRequiredService<AdvDbContext>());
    }

    /// <summary>Direct read on the host connection. Do not call while a transaction is open.</summary>
    public long Scalar(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }

    public List<(int Id, string Payload)> OutboxRows()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Payload\" FROM \"OutboxMessages\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, string)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    /// <summary>Outbox rows whose payload carries a user (UserCreated).</summary>
    public List<(int Id, string Payload)> UserRows() => OutboxRows().Where(r => r.Payload.Contains("\"user\"")).ToList();

    public List<(int Id, string Email)> Users()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Email\" FROM \"Users\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, string)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}
