using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Tester (adversarial) cases for fix-owned-save-drops-events-added-during-save: an owned save removes from the buffer
/// only the events it wrote (by reference and multiplicity); anything else stays buffered and is written once by a later
/// save. No committed event may be written twice; nothing uncommitted may be dispatched.
/// </summary>
public class TesterOwnedSaveEventsAddedDuringSaveTests
{
    public static TheoryData<bool, bool> AsyncFlag()
    {
        var data = new TheoryData<bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(async, flag);
        return data;
    }

    public static TheoryData<bool> Async() => new() { false, true };

    /// <summary>Record event: value equality, distinct instances.</summary>
    public sealed record TesterRecordEvent(string Text);

    private static UserCreated AddUserWithEvent(AdvDbContext db, string email)
    {
        var user = new User { Email = email };
        db.Users.Add(user);
        var evt = new UserCreated { User = user };
        db.Outbox.AddEvent(evt);
        return evt;
    }

    private static async Task SaveAndAccept(AdvDbContext db, bool async, bool flag)
    {
        await Save.Invoke(db, async, flag);
        if (!flag) db.ChangeTracker.AcceptAllChanges();
    }

    private static int RowsWithText(CommitTesterHost host, string text) => host.OutboxRows().Count(r => r.Payload.Contains(text));

    /// <summary>Every dispatched message has a committed row and none is dispatched twice.</summary>
    private static void AssertNoPhantomOrDoubleDispatch(CommitTesterHost host)
    {
        var rowIds = host.OutboxRows().Select(r => r.Id).ToHashSet();
        var enqueued = host.Queue.Enqueued.Select(m => m.Id).ToList();
        Assert.All(enqueued, id => Assert.Contains(id, rowIds));
        Assert.Equal(enqueued.Count, enqueued.Distinct().Count());
    }

    private static async Task SaveUnrelated(AdvDbContext db, bool async, string title = "later")
    {
        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = title });
        await Save.Invoke(db, async);
    }

    /// <summary>A later unrelated save writes and dispatches nothing more and leaves the buffer empty.</summary>
    private static async Task AssertNothingMoreIsWritten(CommitTesterHost host, AdvDbContext db, bool async)
    {
        var rows = host.OutboxRows().Count;
        var dispatched = host.Queue.Enqueued.Count;
        await SaveUnrelated(db, async, "unrelated");
        Assert.Equal(rows, host.OutboxRows().Count);
        Assert.Equal(dispatched, host.Queue.Enqueued.Count);
        Assert.Empty(db.Outbox.GetEvents());
    }

    // ---------------- Buffer manipulation by a handler during the save ----------------

    /// <summary>A handler clears the buffer during the owned save: the snapshot is still written once; nothing is re-written later.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task HandlerClearsBufferDuringOwnedSave_SnapshotWrittenOnce_NothingKept(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.ClearEvents(); };
        AddUserWithEvent(db, "a");

        await SaveAndAccept(db, async, flag);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Empty(db.Outbox.GetEvents());
        Assert.Single(host.Queue.Enqueued);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// A handler clears the buffer then re-adds the instance being saved: the save wrote it once and removes the only
    /// buffered copy, so it is written once in total (the clear + re-add is net "one copy", which the save consumed).
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task HandlerClearsThenReAddsSavedInstance_WrittenOnceInTotal(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var evt = AddUserWithEvent(db, "a");
        var calls = 0;
        db.SavingChanges += (_, _) =>
        {
            if (calls++ != 0) return;
            db.Outbox.ClearEvents();
            db.Outbox.AddEvent(evt);
        };

        await SaveAndAccept(db, async, flag);

        Assert.Single(host.UserRows());
        Assert.Empty(db.Outbox.GetEvents());
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// A handler clears the buffer, adds a new event and then re-adds the saved instance (reordering): only the saved
    /// instance is removed, the new event is kept and written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task HandlerReordersBuffer_NewEventKept_SavedInstanceRemoved(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var evt = AddUserWithEvent(db, "a");
        var during = new PlainEvent { Text = "reordered" };
        var calls = 0;
        db.SavingChanges += (_, _) =>
        {
            if (calls++ != 0) return;
            db.Outbox.ClearEvents();
            db.Outbox.AddEvent(during, evt);
        };

        await SaveAndAccept(db, async, flag);

        Assert.Single(host.UserRows());
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));

        await SaveUnrelated(db, async);

        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "reordered"));
        Assert.Equal(2, host.Queue.Enqueued.Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>A record event equal (by value) to the saved one but a different instance is kept: matching is by reference.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task HandlerAddsValueEqualDifferentInstance_Kept_WrittenOnceByNextSave(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        db.Users.Add(new User { Email = "a" });
        var saved = new TesterRecordEvent("equal");
        db.Outbox.AddEvent(saved);
        var copy = new TesterRecordEvent("equal");
        Assert.Equal(saved, copy);
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(copy); };

        await SaveAndAccept(db, async, flag);

        Assert.Equal(1, RowsWithText(host, "equal"));
        Assert.Same(copy, Assert.Single(db.Outbox.GetEvents()));

        await SaveUnrelated(db, async);

        Assert.Equal(2, RowsWithText(host, "equal"));
        Assert.Equal(2, host.Queue.Enqueued.Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// Same instance buffered twice, handler clears and re-adds it once: two rows were written (snapshot had two copies);
    /// removal of the second copy finds nothing and must not throw or remove anything else.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task SnapshotHasTwoCopies_HandlerLeavesOne_BothWritten_NothingKept(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var evt = AddUserWithEvent(db, "a");
        db.Outbox.AddEvent(evt);
        var calls = 0;
        db.SavingChanges += (_, _) =>
        {
            if (calls++ != 0) return;
            db.Outbox.ClearEvents();
            db.Outbox.AddEvent(evt);
        };

        await SaveAndAccept(db, async, flag);

        Assert.Equal(2, host.UserRows().Count);
        Assert.Empty(db.Outbox.GetEvents());
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Failures after the commit ----------------

    /// <summary>
    /// The in-memory queue throws while the committed messages are enqueued (after the commit): the save throws, but the
    /// buffer was already updated: the saved event is not kept (no duplicate row later) and the event added during the
    /// save is kept and written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task QueueThrowsAfterOwnedCommit_SavedEventNotRewritten_DuringEventWrittenOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        PlainEvent? during = null;
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(during = new PlainEvent { Text = "during save" }); };
        AddUserWithEvent(db, "a");

        host.Queue.FailNext = true;
        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
        Assert.True(ExceptionChain.Contains<QueueFailureException>(error));
        if (!flag) db.ChangeTracker.AcceptAllChanges();

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));

        await SaveUnrelated(db, async);

        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "during save"));
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>Exception thrown by the ChangeTracker.StateChanged handler installed by <see cref="ThrowOnFirstUserAccepted"/>.</summary>
    public sealed class StateChangedHandlerException : Exception
    {
        public StateChangedHandlerException() : base("StateChanged handler failure")
        {
        }
    }

    /// <summary>
    /// Makes AcceptAllChanges throw the first time a User entry becomes Unchanged (a ChangeTracker.StateChanged handler).
    /// Returns a counter of how many times the handler threw.
    /// </summary>
    private static Func<int> ThrowOnFirstUserAccepted(AdvDbContext db)
    {
        var thrown = 0;
        db.ChangeTracker.StateChanged += (_, e) =>
        {
            if (thrown == 0 && e.Entry.Entity is User && e.NewState == EntityState.Unchanged)
            {
                thrown++;
                throw new StateChangedHandlerException();
            }
        };
        return () => thrown;
    }

    /// <summary>The single outbox row's LockExpiresAt, read through a fresh context.</summary>
    private static DateTime? SingleRowLockExpiresAt(CommitTesterHost host)
    {
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        return Assert.Single(db.OutboxMessages.AsNoTracking().ToList()).LockExpiresAt;
    }

    /// <summary>
    /// Bug A fix: when AcceptAllChanges throws after an owned save committed (here a ChangeTracker.StateChanged
    /// handler), the save rethrows the original exception (plain EF parity), but the buffer was already trimmed: the
    /// committed event is not written again by a later save. It is not dispatched in memory (left to the poller, which
    /// delivers the committed row after its LockExpiresAt).
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task AcceptAllChangesThrowsAfterOwnedCommit_CommittedEventNotRewritten_ExceptionPropagates(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var thrown = ThrowOnFirstUserAccepted(db);
        AddUserWithEvent(db, "a");

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        Assert.IsType<StateChangedHandlerException>(error);
        Assert.Equal(1, thrown());
        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Single(host.OutboxRows());
        Assert.Empty(db.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);
        var lockExpiresAt = SingleRowLockExpiresAt(host);
        Assert.NotNull(lockExpiresAt);
        Assert.True(lockExpiresAt > DateTime.UtcNow, "The committed row stays locked; the poller delivers it after LockExpiresAt.");

        db.ChangeTracker.AcceptAllChanges();
        await SaveUnrelated(db, async);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Single(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// AcceptAllChanges throws after an owned commit while a SavingChanges handler added an event during that save: the
    /// saved event is trimmed (written once), the during-save event stays buffered and is written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task AcceptAllChangesThrowsAfterOwnedCommit_DuringSaveEventKept_EachWrittenOnce(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnFirstUserAccepted(db);
        PlainEvent? during = null;
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(during = new PlainEvent { Text = "during save" }); };
        AddUserWithEvent(db, "a");

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        Assert.IsType<StateChangedHandlerException>(error);
        Assert.Single(host.UserRows());
        Assert.Equal(0, RowsWithText(host, "during save"));
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));
        Assert.Empty(host.Queue.Enqueued);

        db.ChangeTracker.AcceptAllChanges();
        await SaveUnrelated(db, async);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Single(host.Queue.Enqueued);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>AcceptAllChanges throws after an owned commit that wrote two events: both are trimmed, each written once.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task AcceptAllChangesThrowsAfterOwnedCommit_TwoEvents_BothTrimmed_EachWrittenOnce(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnFirstUserAccepted(db);
        AddUserWithEvent(db, "a");
        db.Outbox.AddEvent(new PlainEvent { Text = "second event" });

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        Assert.IsType<StateChangedHandlerException>(error);
        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "second event"));
        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Empty(db.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);

        db.ChangeTracker.AcceptAllChanges();
        await SaveUnrelated(db, async);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "second event"));
        Assert.Equal(2, host.OutboxRows().Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// Retrying execution strategy: AcceptAllChanges runs after the strategy returned (after the commit), so its failure
    /// is not retried: one attempt, one commit, one business row, one outbox row; the event is not written again later.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task AcceptAllChangesThrowsAfterOwnedCommit_RetryingStrategy_NotReExecuted(bool async)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnFirstUserAccepted(db);
        var savingCalls = 0;
        db.SavingChanges += (_, _) => savingCalls++;
        AddUserWithEvent(db, "a");
        host.Commits.Reset();

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        Assert.IsType<StateChangedHandlerException>(error);
        Assert.Equal(1, savingCalls);
        Assert.Equal(1, host.Commits.Count);
        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Single(host.OutboxRows());
        Assert.Empty(db.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);

        db.ChangeTracker.AcceptAllChanges();
        await SaveUnrelated(db, async);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// Characterization (caller-transaction path, unaffected by the fix): AcceptAllChanges throws inside the caller
    /// transaction after the message became pending; the save rethrows, the caller commits: one row, dispatched once
    /// on commit, buffer empty, nothing written again later.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task AcceptAllChangesThrowsInCallerTransaction_ThenCommit_WrittenAndDispatchedOnce(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnFirstUserAccepted(db);
        AddUserWithEvent(db, "a");

        using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));
            Assert.IsType<StateChangedHandlerException>(error);
            Assert.Empty(host.Queue.Enqueued);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Single(host.Queue.Enqueued);
        Assert.Empty(db.Outbox.GetEvents());

        db.ChangeTracker.AcceptAllChanges();
        await SaveUnrelated(db, async);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Single(host.Queue.Enqueued);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Retrying execution strategy, failures at different points ----------------

    /// <summary>Retry after the business INSERT fails (inside EF's save): guarded handler's event kept once; user and its event committed once.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task RetryAfterBusinessInsertFails_GuardedHandlerEvent_KeptOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" }); };
        AddUserWithEvent(db, "a");

        host.Faults.FailCommands(SqlMatch.IsUserInsert);
        await SaveAndAccept(db, async, flag);

        Assert.Equal(1, host.Faults.CommandFailuresFired);
        Assert.Equal(2, calls);
        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Single(db.Outbox.GetEvents());

        await SaveUnrelated(db, async);

        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Equal(2, host.OutboxRows().Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>Retry after the payload UPDATE fails (after EF's save, before commit): guarded handler's event kept once; payload carries the real key.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task RetryAfterPayloadUpdateFails_GuardedHandlerEvent_KeptOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" }); };
        AddUserWithEvent(db, "a");

        host.Faults.FailCommands(SqlMatch.IsOutboxUpdate);
        await SaveAndAccept(db, async, flag);

        Assert.Equal(1, host.Faults.CommandFailuresFired);
        var user = Assert.Single(host.Users());
        var row = Assert.Single(host.UserRows());
        Assert.Equal(user.Id, Payload.User(row.Payload).GetProperty("id").GetInt32());
        Assert.Single(db.Outbox.GetEvents());

        await SaveUnrelated(db, async);

        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Equal(2, host.OutboxRows().Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// A SaveChangesFailed handler adds an event when EF's save fails; the retried attempt succeeds. The handler's event
    /// was never part of the save (it did not exist when the events were collected): kept once, written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task SaveChangesFailedHandlerAddsEvent_RetrySucceeds_EventKeptOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var failed = 0;
        db.SaveChangesFailed += (_, _) => { failed++; db.Outbox.AddEvent(new PlainEvent { Text = "on failure" }); };
        AddUserWithEvent(db, "a");

        host.Faults.FailCommands(SqlMatch.IsUserInsert);
        await SaveAndAccept(db, async, flag);

        Assert.Equal(1, failed);
        Assert.Single(host.UserRows());
        Assert.Single(db.Outbox.GetEvents());

        await SaveUnrelated(db, async);

        Assert.Equal(1, RowsWithText(host, "on failure"));
        Assert.Equal(2, host.OutboxRows().Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Events added during the save that writes the kept event ----------------

    /// <summary>
    /// HAZARD (by design): a handler that adds an event on every save keeps the buffer permanently one save behind:
    /// each save writes the previous save's event and keeps its own. Nothing is duplicated; the last one is discarded
    /// with the context.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task HandlerAddsEventOnEverySave_EachSaveWritesPreviousOne_NoDuplicates(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var n = 0;
        db.SavingChanges += (_, _) => db.Outbox.AddEvent(new PlainEvent { Text = "tick-" + (++n) });
        AddUserWithEvent(db, "a");

        await Save.Invoke(db, async);
        await SaveUnrelated(db, async, "2");
        await SaveUnrelated(db, async, "3");

        Assert.Equal(3, n);
        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "tick-1"));
        Assert.Equal(1, RowsWithText(host, "tick-2"));
        Assert.Equal(0, RowsWithText(host, "tick-3"));
        Assert.Equal(3, host.OutboxRows().Count);
        var kept = Assert.IsType<PlainEvent>(Assert.Single(db.Outbox.GetEvents()));
        Assert.Equal("tick-3", kept.Text);
        AssertNoPhantomOrDoubleDispatch(host);
        Assert.Equal(3, host.Queue.Enqueued.Count);
    }

    // ---------------- A kept event then meets the caller-transaction path ----------------

    /// <summary>The kept event is saved in a later caller transaction: written once, dispatched once on commit, removed from the buffer.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task KeptEvent_ThenCallerTransactionCommits_WrittenAndDispatchedOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" }); };
        AddUserWithEvent(db, "a");
        await SaveAndAccept(db, async, flag);
        Assert.Single(db.Outbox.GetEvents());

        using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "in tx" });
            await SaveAndAccept(db, async, flag);
            Assert.Equal(1, host.Queue.Enqueued.Count);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Equal(2, host.Queue.Enqueued.Count);
        Assert.Empty(db.Outbox.GetEvents());
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>The kept event is saved in a caller transaction that rolls back: not dispatched, still buffered, written once by the next owned save.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task KeptEvent_ThenCallerTransactionRollsBack_NotDispatched_WrittenOnceLater(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" }); };
        AddUserWithEvent(db, "a");
        await SaveAndAccept(db, async, flag);

        using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "in tx" });
            await Save.Invoke(db, async, flag);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }
        if (!flag) db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(0, RowsWithText(host, "during save"));
        Assert.Single(host.Queue.Enqueued);
        // flag = true: the rolled-back save accepted the (now lost) business changes, so the event is dropped with
        // them (existing rule); flag = false: kept with its still-pending changes. Either way it is never written twice.
        await SaveUnrelated(db, async);

        Assert.Equal(flag ? 0 : 1, RowsWithText(host, "during save"));
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Pooled context ----------------

    /// <summary>
    /// HAZARD (documented): an event added during the last owned save of a pooled lease is kept only until the lease
    /// ends; the next lease starts with an empty buffer and never writes it.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task PooledLease_EventAddedDuringLastOwnedSave_DiscardedAtLeaseEnd_NotWrittenByNextLease(bool async)
    {
        using var host = PoolHost.Create(ContextRegistration.AddDbContextPool);
        var first = host.Lease();
        var calls = 0;
        EventHandler<SavingChangesEventArgs> handler = (_, _) => { if (calls++ == 0) first.Db.Outbox.AddEvent(new PlainEvent { Text = "during save" }); };
        first.Db.SavingChanges += handler;
        var user = new User { Email = "a" };
        first.Db.Users.Add(user);
        first.Db.Outbox.AddEvent(new UserCreated { User = user });
        await Save.Invoke(first.Db, async);
        first.Db.SavingChanges -= handler;
        Assert.Single(first.Db.Outbox.GetEvents());
        await first.End(async);

        var second = host.Lease();
        Assert.Empty(second.Db.Outbox.GetEvents());
        second.Db.Users.Add(new User { Email = "b" });
        await Save.Invoke(second.Db, async);
        await second.End(async);

        Assert.Single(host.OutboxRows());
        Assert.DoesNotContain(host.OutboxRows(), r => r.Payload.Contains("during save"));
        host.AssertEveryDispatchHasACommittedRow();
    }

    // ---------------- Reentrancy: nested SaveChanges from a handler during an owned save ----------------

    /// <summary>
    /// PRE-EXISTING HAZARD (characterization; identical before and after the fix, confirmed by running it with the
    /// production change stashed): a SavingChanges handler adds an event and calls SaveChanges (nested) during an owned
    /// save. The nested save sees the library's own transaction as the current transaction, takes the caller-transaction
    /// path and saves the whole buffer again, including the event the outer save already collected and added an outbox
    /// entry for. With nested SaveChanges(true) the outer event gets two committed rows; with nested SaveChanges(false)
    /// the outer EF save re-inserts the entries the nested save left Added and fails (DbUpdateException), and later
    /// saves write each event once. Nothing is dispatched without a committed row. Re-entrant saves are not supported.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task PreExistingHazard_NestedSaveFromSavingChangesHandler_OuterEventDuplicatedOrSaveFails(bool async, bool nestedFlag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var calls = 0;
        db.SavingChanges += (_, _) =>
        {
            if (calls++ != 0) return;
            db.Outbox.AddEvent(new PlainEvent { Text = "nested" });
            db.SaveChanges(nestedFlag);
        };
        db.Outbox.AddEvent(new PlainEvent { Text = "outer" });
        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "outer doc" });

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        await SaveUnrelated(db, async, "after-1");
        await SaveUnrelated(db, async, "after-2");

        if (nestedFlag)
        {
            Assert.Null(error);
            Assert.Equal(2, RowsWithText(host, "\"outer\""));
        }
        else
        {
            Assert.IsType<DbUpdateException>(error);
            Assert.Equal(1, RowsWithText(host, "\"outer\""));
        }
        Assert.Equal(1, RowsWithText(host, "\"nested\""));
        Assert.Empty(db.Outbox.GetEvents());
        AssertNoPhantomOrDoubleDispatch(host);
    }
}
