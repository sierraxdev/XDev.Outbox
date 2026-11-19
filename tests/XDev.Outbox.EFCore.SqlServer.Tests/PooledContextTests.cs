using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// AddDbContextPool with a pool of one on SQL Server (on net8.0 this also exercises the wrapped-options registration):
/// the next lease reuses the same instance and must start clean after a failed owned save or an abandoned caller
/// transaction.
/// </summary>
public class PooledContextTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public PooledContextTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    private SmokeHost<int, SmokeDbContext> CreateHost()
        => SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { Registration = Registration.AddDbContextPool, PoolSize = 1 });

    private static async Task End(SmokeScope<SmokeDbContext> lease, bool async)
    {
        if (async) await lease.DisposeAsync(); else lease.Dispose();
    }

    private static async Task<Guid> NextLeaseSavesItsOwnEventOnly(SmokeHost<int, SmokeDbContext> host, Guid expectedInstance, bool async)
    {
        var lease = host.CreateScope();
        try
        {
            Assert.Equal(expectedInstance, lease.Db.ContextId.InstanceId);
            Assert.Null(lease.Db.Database.CurrentTransaction);
            Assert.Empty(lease.Db.Outbox.GetEvents());
            Assert.Empty(lease.Db.ChangeTracker.Entries());

            var user = Graph.NewUser("lease2@test", "l2");
            lease.Db.Users.Add(user);
            lease.Db.Outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(lease.Db, async);
            return lease.Db.ContextId.InstanceId;
        }
        finally
        {
            await End(lease, async);
        }
    }

    private static void AssertOnlySecondLeaseData(SmokeHost<int, SmokeDbContext> host)
    {
        var users = host.Database.Users();
        Assert.Equal("lease2@test", Assert.Single(users).Email);
        var row = Assert.Single(host.OutboxRows());
        GraphAssert.UserPayloadMatches(row.Payload, users, host.Database.Orders());
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id.ToString());
        Assert.Equal("committed", enqueued.RowStateAtEnqueue);
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PooledLease_AfterFailedOwnedSave_NextLeaseOfTheSameInstanceIsCleanAndPersistsOnlyItsOwnEvent(bool async)
    {
        using var host = CreateHost();
        var first = host.CreateScope();
        var instance = first.Db.ContextId.InstanceId;
        var user = Graph.NewUser("lease1@test", "l1");
        first.Db.Users.Add(user);
        first.Db.Outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(sql => SqlMatch.WritesTo(sql, "Orders"), FaultTiming.AfterExecution);
        Assert.NotNull(await Record.ExceptionAsync(() => Save.Invoke(first.Db, async)));
        Assert.Single(first.Db.Outbox.GetEvents());
        await End(first, async);

        await NextLeaseSavesItsOwnEventOnly(host, instance, async);

        AssertOnlySecondLeaseData(host);
    }

    [SqlServerTheory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task PooledLease_CallerTransactionLeftOpen_IsRolledBackAndNextLeaseNeverDispatchesItsPendingMessage(bool accept, bool async)
    {
        using var host = CreateHost();
        var first = host.CreateScope();
        var instance = first.Db.ContextId.InstanceId;
        _ = async ? await first.Db.Database.BeginTransactionAsync() : first.Db.Database.BeginTransaction();
        var user = Graph.NewUser("lease1@test", "l1");
        first.Db.Users.Add(user);
        first.Db.Outbox.AddEvent(new UserCreated { User = user });
        await Save.Invoke(first.Db, async ? (accept ? SaveCall.SaveChangesAsyncTrue : SaveCall.SaveChangesAsyncFalse) : (accept ? SaveCall.SaveChangesTrue : SaveCall.SaveChangesFalse));
        await End(first, async); // no commit, no rollback: the pool resets the context

        await NextLeaseSavesItsOwnEventOnly(host, instance, async);

        AssertOnlySecondLeaseData(host);
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PooledLease_NextLeaseCommitsItsOwnCallerTransaction_DispatchesOnlyItsOwnMessage(bool async)
    {
        using var host = CreateHost();
        var first = host.CreateScope();
        var instance = first.Db.ContextId.InstanceId;
        _ = async ? await first.Db.Database.BeginTransactionAsync() : first.Db.Database.BeginTransaction();
        first.Db.Outbox.AddEvent(new TextEvent { Text = "abandoned" });
        await Save.Invoke(first.Db, async);
        await End(first, async);

        var second = host.CreateScope();
        Assert.Equal(instance, second.Db.ContextId.InstanceId);
        await using (var tx = async ? await second.Db.Database.BeginTransactionAsync() : second.Db.Database.BeginTransaction())
        {
            second.Db.Outbox.AddEvent(new TextEvent { Text = "committed" });
            await Save.Invoke(second.Db, async);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }
        await End(second, async);

        var row = Assert.Single(host.OutboxRows());
        Assert.Contains("committed", row.Payload);
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id.ToString());
    }
}
