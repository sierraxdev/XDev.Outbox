using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Round2;

/// <summary>
/// F1: an event kept buffered because its message was released while its business changes were still pending
/// (saved with acceptAllChangesOnSuccess = false, then rollback / savepoint rollback / disposal) must stay
/// distinguishable from a never-saved event, so a later transaction that ends without saving it does not drop it.
/// It is forgotten once saved again, once its changes are accepted, or once the application removes it from the buffer.
/// </summary>
public class ReleasedEventTrackingTests
{
    public static TheoryData<bool> Async => new() { false, true };

    /// <summary>(a) save(false) -> rollback; tx2 rolls back without saving; tx3 saves and commits.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task ReleasedEvent_LaterTransactionRolledBackWithoutSave_EventStillPersistedOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var (user, evt) = AddUserWithEvent(scope);

        await SaveNotAcceptedThenRollback(scope, async);

        using (var tx = await Begin(scope.Db, async))
        {
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));

        await SaveAcceptedInTransactionAndCommit(scope, async);

        AssertPersistedAndDispatchedOnce(host, scope, user);
    }

    /// <summary>(b) save(false) -> rollback; tx2 commits without saving; tx3 saves and commits.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task ReleasedEvent_LaterTransactionCommittedWithoutSave_EventStillPersistedOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var (user, evt) = AddUserWithEvent(scope);

        await SaveNotAcceptedThenRollback(scope, async);

        using (var tx = await Begin(scope.Db, async))
        {
            await Commit(tx, async);
        }

        Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));
        Assert.Empty(host.Queue.Enqueued);

        await SaveAcceptedInTransactionAndCommit(scope, async);

        AssertPersistedAndDispatchedOnce(host, scope, user);
    }

    /// <summary>
    /// Disposal variant: tx1 is disposed without commit (stale), tx2 commits without saving (stale release on commit),
    /// tx3 commits without saving; a later owned save still persists the event once.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task StaleReleasedEvent_TwoLaterTransactionsCommittedWithoutSave_EventStillPersistedOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var (user, evt) = AddUserWithEvent(scope);

        using (await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
        }

        for (var i = 0; i < 2; i++)
        {
            using var tx = await Begin(scope.Db, async);
            await Commit(tx, async);
        }

        Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));
        Assert.Empty(host.Queue.Enqueued);

        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);

        AssertPersistedAndDispatchedOnce(host, scope, user);
    }

    /// <summary>
    /// (c) save(false) -> rollback to a savepoint releases the message -> the caller commits without saving again;
    /// a later save persists the event exactly once with the business row.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task SavepointReleasedEvent_CallerCommitsWithoutResave_LaterSavePersistsOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var (user, evt) = AddUserWithEvent(scope);

        using (var tx = await Begin(scope.Db, async))
        {
            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
            await Commit(tx, async);
        }

        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
        Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));

        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);

        AssertPersistedAndDispatchedOnce(host, scope, user);
    }

    /// <summary>
    /// (d) save(false) -> rollback; tx2 rolls back without saving; an owned flag = true save of an unrelated entity
    /// also saves the still-pending business changes and their event, exactly once. Afterwards the event is gone:
    /// another transaction ending without a save and a later save write nothing more.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task ReleasedEvent_OwnedAcceptingSaveOfUnrelatedEntity_PersistsEventOnce_ThenForgotten(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var (user, _) = AddUserWithEvent(scope);

        await SaveNotAcceptedThenRollback(scope, async);
        using (var tx = await Begin(scope.Db, async))
        {
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        scope.Db.VersionedItems.Add(new Versioned { Name = "unrelated" });
        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);

        Assert.Equal(1, host.Count("VersionedItems"));
        AssertPersistedAndDispatchedOnce(host, scope, user);

        using (var tx = await Begin(scope.Db, async))
        {
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }
        scope.Db.VersionedItems.Add(new Versioned { Name = "later" });
        await Save.Invoke(scope.Db, async);

        Assert.Equal(1, host.Count("OutboxMessages"));
        Assert.Single(host.Queue.Enqueued);
        Assert.Empty(scope.Outbox.GetEvents());
    }

    /// <summary>
    /// (e) save(false) -> rollback -> the application clears the buffer: no outbox row is written later, also when a
    /// later transaction ends without a save and the same instance is not re-added.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task ReleasedEvent_ApplicationClearsBuffer_NoResurrection(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        AddUserWithEvent(scope);

        await SaveNotAcceptedThenRollback(scope, async);
        scope.Outbox.ClearEvents();

        using (var tx = await Begin(scope.Db, async))
        {
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }
        Assert.Empty(scope.Outbox.GetEvents());

        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);

        Assert.Equal(1, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(scope.Outbox.GetEvents());
    }

    /// <summary>
    /// No phantom: the released event is saved again by an accepting save in tx2, which then rolls back. The changes
    /// were accepted, so the event is dropped; a later transaction ending without a save and a later save write nothing.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task ReleasedEvent_ResavedWithAcceptInRolledBackTransaction_IsDropped_NoPhantom(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        AddUserWithEvent(scope);

        await SaveNotAcceptedThenRollback(scope, async);

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }
        Assert.Empty(scope.Outbox.GetEvents());

        using (var tx = await Begin(scope.Db, async))
        {
            await Commit(tx, async);
        }

        scope.Db.VersionedItems.Add(new Versioned { Name = "unrelated" });
        await Save.Invoke(scope.Db, async);

        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(scope.Outbox.GetEvents());
    }

    /// <summary>
    /// No phantom: the application clears the buffer of a released event, an accepting save without events accepts
    /// the pending changes, then the same instance is re-added: it is a fresh (never-saved) event, dropped by a
    /// rollback like any other never-saved event.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task ReleasedEvent_ClearedThenChangesAcceptedThenReAdded_TreatedAsNeverSaved(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var (_, evt) = AddUserWithEvent(scope);

        await SaveNotAcceptedThenRollback(scope, async);
        scope.Outbox.ClearEvents();

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);
            scope.Outbox.AddEvent(evt);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Empty(scope.Outbox.GetEvents());
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// Duplicates: the same instance is buffered twice and both copies are released; a later transaction ending
    /// without a save keeps both, and the retry writes two rows (as the original save did).
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task ReleasedDuplicateEvent_LaterTransactionRolledBackWithoutSave_BothCopiesKept(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var (_, evt) = AddUserWithEvent(scope);
        scope.Outbox.AddEvent(evt);

        await SaveNotAcceptedThenRollback(scope, async);
        using (var tx = await Begin(scope.Db, async))
        {
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        var buffered = scope.Outbox.GetEvents();
        Assert.Equal(2, buffered.Count);
        Assert.All(buffered, x => Assert.Same(evt, x));

        await SaveAcceptedInTransactionAndCommit(scope, async);

        Assert.Single(host.UserIds());
        Assert.Equal(2, host.Count("OutboxMessages"));
        Assert.Equal(2, host.Queue.Enqueued.Count);
        Assert.Empty(scope.Outbox.GetEvents());
    }

    private static (User User, UserCreated Event) AddUserWithEvent(TestScope scope)
    {
        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        var evt = new UserCreated { User = user };
        scope.Outbox.AddEvent(evt);
        return (user, evt);
    }

    private static async Task SaveNotAcceptedThenRollback(TestScope scope, bool async)
    {
        using var tx = await Begin(scope.Db, async);
        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
        if (async) await tx.RollbackAsync(); else tx.Rollback();
    }

    private static async Task SaveAcceptedInTransactionAndCommit(TestScope scope, bool async)
    {
        using var tx = await Begin(scope.Db, async);
        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);
        await Commit(tx, async);
    }

    private static void AssertPersistedAndDispatchedOnce(OutboxTestHost host, TestScope scope, User user)
    {
        var userId = Assert.Single(host.UserIds());
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(userId, Payload.User(row.Payload).GetProperty("id").GetInt32());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
        Assert.Equal(userId, user.Id);
        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        Assert.Empty(scope.Outbox.GetEvents());
    }

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }
}
