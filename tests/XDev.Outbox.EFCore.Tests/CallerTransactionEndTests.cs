using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// B1: when a caller-owned transaction ends without a commit, a buffered event follows the business changes it was
/// saved with. acceptAllChangesOnSuccess = false leaves those changes pending, so the event stays buffered and a
/// retry saves both exactly once; acceptAllChangesOnSuccess = true accepted them, so the event is dropped (also when
/// the end is only detected as a stale transaction on the next save). Rolling back to a caller savepoint applies the
/// same rule to the messages whose rows it undid.
/// </summary>
public class CallerTransactionEndTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSaveNotAccepted_CallerRollsBack_RetryInNewTransactionPersistsAndDispatchesOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        var evt = new UserCreated { User = user };
        scope.Outbox.AddEvent(evt);

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Empty(host.Queue.Enqueued);
        Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));
        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);

        await RetryInTransactionAndAccept(scope, async);

        AssertPersistedAndDispatchedOnce(host, scope, user);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSaveNotAccepted_CallerCommitFails_RetryInNewTransactionPersistsAndDispatchesOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        var evt = new UserCreated { User = user };
        scope.Outbox.AddEvent(evt);
        host.Faults.FailCommits();

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
            await Assert.ThrowsAnyAsync<Exception>(() => Commit(tx, async));
        }

        Assert.Equal(1, host.Faults.CommitFailuresFired);
        Assert.Empty(host.Queue.Enqueued);
        Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));
        Assert.Equal(0, host.Count("Users"));

        await RetryInTransactionAndAccept(scope, async);

        AssertPersistedAndDispatchedOnce(host, scope, user);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSaveAccepted_TransactionDisposedWithoutCommit_LaterSaveWithoutTransactionWritesNoOutboxRow(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        using (await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);
            // Disposed without commit or rollback: EF raises no event; the next save detects it.
        }

        scope.Db.VersionedItems.Add(new Versioned { Name = "unrelated" });
        await Save.Invoke(scope.Db, async);

        Assert.Equal(1, host.Count("VersionedItems"));
        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(scope.Outbox.GetEvents());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSaveAccepted_CallerRollsBack_ClearsBufferAndDispatchesNothing(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Empty(scope.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(0, host.Count("OutboxMessages"));

        // A later save writes nothing for the dropped event.
        scope.Db.VersionedItems.Add(new Versioned { Name = "unrelated" });
        await Save.Invoke(scope.Db, async);
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// Events added to the buffer but never saved in the transaction are kept by a rollback (as by a dispose without
    /// rollback), together with events saved with acceptAllChangesOnSuccess = false. (Before
    /// fix-rollback-event-loss-and-retry-identity-conflict, the rollback cleared never-saved events.)
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSaveNotAccepted_CallerRollsBack_KeepsSavedEventAndNeverSavedEvent(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        var saved = new UserCreated { User = user };
        var never = new UserCreated();
        scope.Outbox.AddEvent(saved);

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
            scope.Outbox.AddEvent(never);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Equal(new object[] { saved, never }, scope.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSaveNotAccepted_RolledBackToCallerSavepoint_SaveAgainAndCommitPersistsAndDispatchesOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        var evt = new UserCreated { User = user };
        scope.Outbox.AddEvent(evt);

        using (var tx = await Begin(scope.Db, async))
        {
            if (async) await tx.CreateSavepointAsync("before"); else tx.CreateSavepoint("before");
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
            if (async) await tx.RollbackToSavepointAsync("before"); else tx.RollbackToSavepoint("before");

            Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));

            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
            await Commit(tx, async);
        }
        scope.Db.ChangeTracker.AcceptAllChanges();

        AssertPersistedAndDispatchedOnce(host, scope, user);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task AmbientSave_RolledBackToSavepointTakenBetweenTwoSaves_CommitDispatchesOnlyTheEarlierMessage(bool async, bool flag)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var first = new User { Email = "first@test" };
        var second = new User { Email = "second@test" };

        using (var tx = await Begin(scope.Db, async))
        {
            scope.Db.Users.Add(first);
            scope.Outbox.AddEvent(new UserCreated { User = first });
            await Save.Invoke(scope.Db, async, flag);

            if (async) await tx.CreateSavepointAsync("middle"); else tx.CreateSavepoint("middle");

            scope.Db.Users.Add(second);
            var secondEvent = new UserCreated { User = second };
            scope.Outbox.AddEvent(secondEvent);
            await Save.Invoke(scope.Db, async, flag);

            if (async) await tx.RollbackToSavepointAsync("middle"); else tx.RollbackToSavepoint("middle");

            // The earlier event is still pending in the transaction; the rolled-back one follows the flag.
            var buffered = scope.Outbox.GetEvents();
            Assert.Equal(flag ? 1 : 2, buffered.Count);
            Assert.Equal(!flag, buffered.Contains(secondEvent));

            await Commit(tx, async);
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal("first@test", Payload.User(row.Payload).GetProperty("email").GetString());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
        Assert.Single(host.UserIds());
    }

    private static async Task RetryInTransactionAndAccept(TestScope scope, bool async)
    {
        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);
            await Commit(tx, async);
        }
        scope.Db.ChangeTracker.AcceptAllChanges();
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
