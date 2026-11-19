using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// Caller-owned (ambient) transaction: the outbox row is written inside the caller's transaction and dispatched
/// only when the caller commits. Uses the default non-retrying execution strategy.
/// </summary>
public class AmbientTransactionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_EnqueuesNothingBeforeCommit_AndExactlyOnceAfterCommit(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async);
            Assert.Empty(host.Queue.Enqueued);
            await Commit(tx, async);
        }

        var row = Assert.Single(host.OutboxRows());
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);
        Assert.NotEqual(0, enqueued.Id);
        Assert.Single(host.UserIds());
        Assert.Empty(scope.Outbox.GetEvents());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_WithAcceptTrue_EntriesAreUnchangedBeforeCallerCommits(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        using var tx = await Begin(scope.Db, async);
        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: true);

        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        Assert.NotEqual(0, user.Id);
        Assert.Equal(0, scope.TrackedOutboxMessages);

        await Commit(tx, async);
        Assert.Single(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_WhenCallerRollsBack_EnqueuesNothingClearsBufferAndTracksNoOutboxEntity(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(scope.Outbox.GetEvents());
        Assert.Equal(0, scope.TrackedOutboxMessages);
        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_WhenTransactionDisposedWithoutCommit_LaterCommitDoesNotEnqueueStaleMessage(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        using (await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async);
            // Disposed without commit or rollback: EF raises no rollback event.
        }

        Assert.Equal(0, host.Count("OutboxMessages"));

        // A later, unrelated transaction on the same context commits without saving anything.
        using (var tx = await Begin(scope.Db, async))
        {
            await Commit(tx, async);
        }

        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(0, host.Count("OutboxMessages"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_TwoSavesInOneTransactionWithSameBufferedEvent_WritesOneRowAndEnqueuesOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        using (var tx = await Begin(scope.Db, async))
        {
            await Save.Invoke(scope.Db, async);
            scope.Db.VersionedItems.Add(new Versioned { Name = "x" });
            await Save.Invoke(scope.Db, async);
            await Commit(tx, async);
        }

        Assert.Equal(1, host.Count("VersionedItems"));
        var row = Assert.Single(host.OutboxRows());
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);
    }

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }
}
