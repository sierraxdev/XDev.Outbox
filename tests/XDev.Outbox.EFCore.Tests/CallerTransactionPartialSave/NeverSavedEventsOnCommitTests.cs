using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// fix-commit-drops-never-saved-events: a commit of a caller transaction keeps buffered events that were never saved
/// in it (no pending message of the committed transaction), as every end without commit does, so they follow their
/// still-pending business changes. Events committed in it are still removed and dispatched. An event leaves the
/// buffer only by a successful save-and-commit, the B1 rule for saved events, Outbox.ClearEvents() or the end of the
/// context/pool lease.
/// </summary>
public class NeverSavedEventsOnCommitTests
{
    public enum Scenario
    {
        /// <summary>Added before the transaction; the transaction commits after raw SQL only (no save).</summary>
        AddedBeforeTransaction,

        /// <summary>Added inside the transaction after its last save (which saved another user and event).</summary>
        AddedAfterLastSave,

        /// <summary>The save with the event failed (undone by the library's savepoint), then the caller commits.</summary>
        FailedSave,

        /// <summary>Kept by an earlier rollback (after a failed save); a later transaction commits without saving it.</summary>
        KeptByEarlierRollback,
    }

    public static TheoryData<Scenario, bool, bool> ScenarioAsyncFlag()
    {
        var data = new TheoryData<Scenario, bool, bool>();
        foreach (var scenario in Enum.GetValues<Scenario>())
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(scenario, async, flag);
        return data;
    }

    public static TheoryData<bool> Async() => new() { false, true };

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static async Task Rollback(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.RollbackAsync(); else tx.Rollback();
    }

    private static async Task RawSql(DbContext db, bool async)
    {
        const string sql = "DELETE FROM \"Docs\"";
        if (async) await db.Database.ExecuteSqlRawAsync(sql); else db.Database.ExecuteSqlRaw(sql);
    }

    private static User AddUserWithEvent(AdvDbContext db, string email, out UserCreated evt)
    {
        var user = new User { Email = email };
        db.Users.Add(user);
        evt = new UserCreated { User = user };
        db.Outbox.AddEvent(evt);
        return user;
    }

    /// <summary>A save whose outbox payload UPDATE runs and then fails (undone by the library's savepoint in a caller transaction).</summary>
    private static async Task FailingSave(RollbackTesterHost host, AdvDbContext db, bool async, bool flag)
    {
        host.Probe.FailAfterExecution(SqlMatch.IsOutboxUpdate);
        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
        Assert.True(ExceptionChain.Contains<InjectedFailureException>(error), error?.ToString());
    }

    /// <summary>Committed users are exactly <paramref name="emails"/>, one outbox row each carrying its user's key, each row dispatched exactly once.</summary>
    private static void AssertCommittedWithOneRowEach(RollbackTesterHost host, params string[] emails)
    {
        var users = host.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        Assert.Equal(
            users.OrderBy(u => u.Email).Select(u => (u.Id, u.Email)),
            rows.Select(r => Payload.User(r.Payload)).Select(p => (p.GetProperty("id").GetInt32(), p.GetProperty("email").GetString()!)).OrderBy(x => x.Item2));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id).OrderBy(x => x));
    }

    /// <summary>
    /// The four scenarios: the never-saved event stays buffered across the commit, has no row and is not dispatched
    /// by it; a later save commits the user WITH its outbox row, dispatched once. In AddedAfterLastSave the event
    /// saved in the transaction is committed, dispatched once by the commit and removed from the buffer.
    /// (Before the fix the commit dropped the event and the later save committed the user without an outbox row.)
    /// </summary>
    [Theory]
    [MemberData(nameof(ScenarioAsyncFlag))]
    public async Task NeverSavedEventThenCommit_EventKept_LaterSaveCommitsUserWithItsRowOnce(Scenario scenario, bool async, bool flag)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        UserCreated never = null!;
        User? saved = null;
        if (scenario == Scenario.AddedBeforeTransaction) AddUserWithEvent(db, "never", out never);
        if (scenario == Scenario.KeptByEarlierRollback)
        {
            AddUserWithEvent(db, "never", out never);
            using var earlier = await Begin(db, async);
            await FailingSave(host, db, async, flag);
            await Rollback(earlier, async);
            Assert.Equal(new object[] { never }, db.Outbox.GetEvents());
        }

        using (var tx = await Begin(db, async))
        {
            switch (scenario)
            {
                case Scenario.AddedBeforeTransaction:
                case Scenario.KeptByEarlierRollback:
                    await RawSql(db, async);
                    break;
                case Scenario.AddedAfterLastSave:
                    saved = AddUserWithEvent(db, "saved", out _);
                    await Save.Invoke(db, async, flag);
                    AddUserWithEvent(db, "never", out never);
                    break;
                case Scenario.FailedSave:
                    AddUserWithEvent(db, "never", out never);
                    await FailingSave(host, db, async, flag);
                    break;
            }
            await Commit(tx, async);
        }

        // Kept, not written and not dispatched by this commit.
        Assert.Equal(new object[] { never }, db.Outbox.GetEvents());
        Assert.DoesNotContain(host.Users(), u => u.Email == "never");
        if (scenario == Scenario.AddedAfterLastSave)
        {
            AssertCommittedWithOneRowEach(host, "saved");
            // SaveChanges(false) + commit: the application accepts the saved user's changes (here only that user,
            // so the never-saved user stays Added).
            if (!flag) db.Entry(saved!).State = EntityState.Unchanged;
        }
        else
        {
            Assert.Empty(host.OutboxRows());
            Assert.Empty(host.Queue.Enqueued);
        }

        await Save.Invoke(db, async);

        if (scenario == Scenario.AddedAfterLastSave) AssertCommittedWithOneRowEach(host, "saved", "never");
        else AssertCommittedWithOneRowEach(host, "never");
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// The never-saved event is kept across several commits (no save; raw SQL; a failed save), then a save in a caller
    /// transaction persists it exactly once and its commit dispatches it once.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task NeverSavedEventKeptAcrossSeveralCommits_ThenSavedAndDispatchedExactlyOnce(bool async)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        AddUserWithEvent(db, "a", out var evt);

        using (var tx = await Begin(db, async))
        {
            await Commit(tx, async);
        }
        Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        using (var tx = await Begin(db, async))
        {
            await RawSql(db, async);
            await Commit(tx, async);
        }
        Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        using (var tx = await Begin(db, async))
        {
            await FailingSave(host, db, async, flag: false);
            await Commit(tx, async);
        }
        Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);

        using (var tx = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            Assert.Empty(host.Queue.Enqueued);
            await Commit(tx, async);
        }

        AssertCommittedWithOneRowEach(host, "a");
        Assert.Empty(db.Outbox.GetEvents());

        // Nothing more is written or dispatched by later transactions and saves.
        using (var tx = await Begin(db, async))
        {
            await Commit(tx, async);
        }
        db.Users.Add(new User { Email = "next" });
        await Save.Invoke(db, async);
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    /// <summary>
    /// The application abandons its unit of work after the commit kept the event: it discards the business change
    /// (ChangeTracker.Clear()) and the event (Outbox.ClearEvents()); a later save persists nothing of it.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task NeverSavedEventKeptAtCommit_AppAbandonsChangesAndClearsEvents_NothingPersisted(bool async)
    {
        using var host = RollbackTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        using (var tx = await Begin(db, async))
        {
            AddUserWithEvent(db, "abandoned", out var evt);
            await FailingSave(host, db, async, flag: true);
            await Commit(tx, async);
            Assert.Equal(new object[] { evt }, db.Outbox.GetEvents());
        }

        db.ChangeTracker.Clear();
        db.Outbox.ClearEvents();

        db.Users.Add(new User { Email = "unrelated" });
        await Save.Invoke(db, async);

        Assert.Equal(new[] { "unrelated" }, host.Users().Select(u => u.Email));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// A never-saved event kept by a commit on a pooled context is discarded at the end of the lease: the next lease
    /// of the same instance starts with an empty buffer and persists and dispatches only its own event.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task PooledContext_EventKeptByCommit_NextLeaseOfSameInstanceNeverSeesIt(bool async)
    {
        using var host = RollbackTesterHost.Create(pooled: true);
        AdvDbContext first;
        {
            var (scope, db) = host.CreateScope();
            using var _s = scope;
            first = db;
            using (var tx = await Begin(db, async))
            {
                AddUserWithEvent(db, "lease1", out _);
                await Commit(tx, async);
            }
            Assert.Single(db.Outbox.GetEvents());
        }

        {
            var (scope, db) = host.CreateScope();
            using var _s = scope;
            Assert.Same(first, db);
            Assert.Empty(db.Outbox.GetEvents());
            Assert.Empty(db.ChangeTracker.Entries());

            using (var tx = await Begin(db, async))
            {
                AddUserWithEvent(db, "lease2", out _);
                await Save.Invoke(db, async);
                await Commit(tx, async);
            }
        }

        AssertCommittedWithOneRowEach(host, "lease2");
    }
}
