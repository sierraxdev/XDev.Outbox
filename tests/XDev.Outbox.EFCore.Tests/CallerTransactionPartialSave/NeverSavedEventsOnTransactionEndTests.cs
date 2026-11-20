using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// fix-rollback-event-loss-and-retry-identity-conflict, bug 1: when a caller transaction ends without a commit
/// (explicit Rollback, failed commit, or a dispose without Rollback, detected as stale at the next save), the buffer
/// outcome is the same. Events with no pending message in the ended transaction (never saved: the save failed, they
/// were added after the last save, or before the transaction) stay buffered and follow their still-pending business
/// changes. B1 is unchanged for saved events: saved with acceptAllChangesOnSuccess = false -> kept; true -> dropped.
/// The commit path is unchanged and pinned at the end of this class: never-saved events are dropped on commit.
/// </summary>
public class NeverSavedEventsOnTransactionEndTests
{
    public enum EndMode
    {
        Rollback,
        Dispose,
        FailedCommit,
    }

    public static TheoryData<EndMode, bool> EndAsync()
    {
        var data = new TheoryData<EndMode, bool>();
        foreach (var end in Enum.GetValues<EndMode>())
        foreach (var async in new[] { false, true })
            data.Add(end, async);
        return data;
    }

    public static TheoryData<EndMode, bool, bool> EndAsyncFlag()
    {
        var data = new TheoryData<EndMode, bool, bool>();
        foreach (var end in Enum.GetValues<EndMode>())
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(end, async, flag);
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

    /// <summary>
    /// Ends the transaction without a commit. For Rollback and FailedCommit the buffer is updated now and checked
    /// against <paramref name="expectedBuffer"/>; a dispose raises no EF event, so the buffer is updated at the next save.
    /// </summary>
    private static async Task End(TesterSavepointHost host, AdvDbContext db, IDbContextTransaction tx, EndMode end, bool async, IReadOnlyList<object> bufferBefore, IReadOnlyList<object> expectedBuffer)
    {
        switch (end)
        {
            case EndMode.Rollback:
                if (async) await tx.RollbackAsync(); else tx.Rollback();
                break;
            case EndMode.FailedCommit:
                host.Faults.FailCommits();
                Assert.True(ExceptionChain.Contains<InjectedFailureException>(await Record.ExceptionAsync(() => Commit(tx, async))));
                Assert.Equal(1, host.Faults.CommitFailuresFired);
                break;
        }
        if (async) await tx.DisposeAsync(); else tx.Dispose();

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(end == EndMode.Dispose ? bufferBefore : expectedBuffer, db.Outbox.GetEvents());
    }

    /// <summary>Saves in a new transaction (accepting) and commits.</summary>
    private static async Task RetryInNewTransaction(AdvDbContext db, bool async)
    {
        using var retry = await Begin(db, async);
        await Save.Invoke(db, async);
        await Commit(retry, async);
    }

    /// <summary>Committed users are exactly <paramref name="emails"/>, one outbox row each carrying its user's key, each dispatched once.</summary>
    private static void AssertCommittedWithOneRowEach(TesterSavepointHost host, AdvDbContext db, params string[] emails)
    {
        var users = host.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        Assert.Equal(users.Select(u => u.Id).OrderBy(x => x), rows.Select(r => Payload.User(r.Payload).GetProperty("id").GetInt32()).OrderBy(x => x));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id).OrderBy(x => x));
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>The only save in the transaction fails (the library's savepoint undoes it): its event was never saved.</summary>
    [Theory]
    [MemberData(nameof(EndAsyncFlag))]
    public async Task FailedSaveThenEndWithoutCommit_EventKept_RetryCommitsUserWithItsRowOnce(EndMode end, bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, _) = host.CreateScope();
        using var _s = scope;

        var user = AddUserWithEvent(db, "a", out var evt);
        var tx = await Begin(db, async);
        host.Probe.FailAfterExecution(SqlMatch.IsOutboxUpdate);
        Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
        Assert.Equal(EntityState.Added, db.Entry(user).State);

        await End(host, db, tx, end, async, new object[] { evt }, new object[] { evt });

        await RetryInNewTransaction(db, async);
        AssertCommittedWithOneRowEach(host, db, "a");
    }

    /// <summary>Events added before the transaction and inside it, never saved (no save in the transaction).</summary>
    [Theory]
    [MemberData(nameof(EndAsync))]
    public async Task EventsAddedButNeverSavedThenEndWithoutCommit_EventsKept_RetryCommitsUsersWithTheirRowsOnce(EndMode end, bool async)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, _) = host.CreateScope();
        using var _s = scope;

        AddUserWithEvent(db, "before", out var before);
        var tx = await Begin(db, async);
        AddUserWithEvent(db, "inside", out var inside);

        await End(host, db, tx, end, async, new object[] { before, inside }, new object[] { before, inside });

        await RetryInNewTransaction(db, async);
        AssertCommittedWithOneRowEach(host, db, "before", "inside");
    }

    /// <summary>
    /// Saved with flag = true (accepted: dropped), saved with flag = false (pending: kept), a failed save (never saved:
    /// kept) and an event added after the last save (never saved: kept), in one transaction that ends without commit.
    /// </summary>
    [Theory]
    [MemberData(nameof(EndAsync))]
    public async Task MixOfSavedAndNeverSavedEventsThenEndWithoutCommit_SavedFollowB1AndNeverSavedKept(EndMode end, bool async)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, _) = host.CreateScope();
        using var _s = scope;

        var tx = await Begin(db, async);
        var accepted = AddUserWithEvent(db, "accepted", out var acceptedEvent);
        await Save.Invoke(db, async, true);
        AddUserWithEvent(db, "pending", out var pendingEvent);
        await Save.Invoke(db, async, false);
        var failed = AddUserWithEvent(db, "failed", out var failedEvent);
        host.Probe.FailAfterExecution(SqlMatch.IsOutboxUpdate);
        Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async, false)));
        Assert.Equal(EntityState.Added, db.Entry(failed).State);
        AddUserWithEvent(db, "later", out var laterEvent);

        await End(
            host,
            db,
            tx,
            end,
            async,
            new object[] { acceptedEvent, pendingEvent, failedEvent, laterEvent },
            new object[] { pendingEvent, failedEvent, laterEvent });

        // The accepted user's changes were accepted by its save: they are not saved again (as with plain EF). Its
        // tracked key was never committed and SQLite hands it out again, so the application detaches it (plain EF).
        Assert.Equal(EntityState.Unchanged, db.Entry(accepted).State);
        db.Entry(accepted).State = EntityState.Detached;
        await RetryInNewTransaction(db, async);
        AssertCommittedWithOneRowEach(host, db, "pending", "failed", "later");
    }

    // ---------------- Commit path: unchanged, pinned ----------------

    public enum NeverSavedOnCommit
    {
        /// <summary>Added before the transaction; the transaction commits without any save.</summary>
        AddedBeforeTransaction,

        /// <summary>Added inside the transaction after its last save.</summary>
        AddedAfterLastSave,

        /// <summary>The save with the event failed (undone by the library's savepoint), then the caller commits.</summary>
        FailedSave,
    }

    public static TheoryData<NeverSavedOnCommit, bool> CommitAsync()
    {
        var data = new TheoryData<NeverSavedOnCommit, bool>();
        foreach (var scenario in Enum.GetValues<NeverSavedOnCommit>())
        foreach (var async in new[] { false, true })
            data.Add(scenario, async);
        return data;
    }

    /// <summary>
    /// PINNED CURRENT BEHAVIOR (not changed by this fix; reported as a hazard): a commit removes never-saved events
    /// from the buffer although their business changes are still pending in the tracker, so a later save commits the
    /// user WITHOUT an outbox row.
    /// </summary>
    [Theory]
    [MemberData(nameof(CommitAsync))]
    public async Task NeverSavedEventThenCommit_EventDropped_LaterSaveCommitsUserWithoutOutboxRow(NeverSavedOnCommit scenario, bool async)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, _) = host.CreateScope();
        using var _s = scope;

        if (scenario == NeverSavedOnCommit.AddedBeforeTransaction) AddUserWithEvent(db, "never", out _);
        using (var tx = await Begin(db, async))
        {
            if (scenario == NeverSavedOnCommit.AddedAfterLastSave)
            {
                AddUserWithEvent(db, "saved", out _);
                await Save.Invoke(db, async);
                AddUserWithEvent(db, "never", out _);
            }
            if (scenario == NeverSavedOnCommit.FailedSave)
            {
                AddUserWithEvent(db, "never", out _);
                host.Probe.FailAfterExecution(SqlMatch.IsOutboxUpdate);
                Assert.IsType<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            }
            await Commit(tx, async);
        }

        Assert.Empty(db.Outbox.GetEvents());
        var dispatchedBefore = host.Queue.Enqueued.Count;

        await Save.Invoke(db, async);

        Assert.Contains(host.Users(), u => u.Email == "never");
        Assert.Equal(scenario == NeverSavedOnCommit.AddedAfterLastSave ? 1 : 0, host.OutboxRows().Count);
        Assert.Equal(dispatchedBefore, host.Queue.Enqueued.Count);
    }
}
