using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>
/// Representative behaviors of the existing suites (all four overloads, flag=false then commit, D-C, savepoint
/// rollback, owned failure + app retry with generated keys / Rule A) run on a reused pooled instance (lease 2+), and on
/// factories, so lease handling cannot hide behind lease 0.
/// </summary>
public class PooledBehaviorParityTests
{
    private static readonly ContextRegistration[] Registrations =
    {
        ContextRegistration.AddDbContextPool,
        ContextRegistration.PooledFactory,
        ContextRegistration.Factory,
        ContextRegistration.FactoryScoped,
    };

    public static TheoryData<ContextRegistration, SaveCall> RegistrationsAndOverloads()
    {
        var data = new TheoryData<ContextRegistration, SaveCall>();
        foreach (var r in Registrations)
        foreach (var call in Enum.GetValues<SaveCall>())
            data.Add(r, call);
        return data;
    }

    public static TheoryData<ContextRegistration, bool> RegistrationsAndAsync()
    {
        var data = new TheoryData<ContextRegistration, bool>();
        foreach (var r in Registrations)
        {
            data.Add(r, false);
            data.Add(r, true);
        }
        return data;
    }

    /// <summary>Uses (and returns) one lease so the next lease of a pooled registration is the reused instance.</summary>
    private static async Task<PoolLease> WarmLease(PoolHost host)
    {
        var warm = host.Lease();
        warm.Db.Outbox.AddEvent(new UserCreated { User = new User { Email = "warm@test" } });
        await warm.End(asyncDispose: false);
        var lease = host.Lease();
        if (host.IsPooled) Assert.True(lease.Db.ContextId.Lease >= 2, "Precondition: reused pooled instance.");
        return lease;
    }

    [Theory]
    [MemberData(nameof(RegistrationsAndOverloads))]
    public async Task ReusedContext_EverySaveOverload_PersistsAndDispatchesOnceWithGeneratedKey(ContextRegistration registration, SaveCall call)
    {
        using var host = PoolHost.Create(registration);
        var lease = await WarmLease(host);
        try
        {
            var user = new User { Email = "a@test", Orders = { new Order { Description = "o" } } };
            lease.Db.Users.Add(user);
            lease.Db.Outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(lease.Db, call);

            var row = Assert.Single(host.OutboxRows());
            Assert.Equal(user.Id, Payload.User(row.Payload).GetProperty("id").GetInt32());
            Assert.NotEqual(0, user.Id);
            Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
            Assert.Empty(lease.Db.Outbox.GetEvents());
            Assert.Equal(EntityState.Unchanged, lease.Db.Entry(user).State);
        }
        finally
        {
            await lease.End(false);
        }
    }

    [Theory]
    [MemberData(nameof(RegistrationsAndAsync))]
    public async Task ReusedContext_DC_FlagTrueSaveAcceptsEarlierFlagFalse_RollbackDropsBothEvents(ContextRegistration registration, bool async)
    {
        using var host = PoolHost.Create(registration);
        var lease = await WarmLease(host);
        try
        {
            var db = lease.Db;
            using (var tx = await PoolReuseAdversarialTests.Begin(db, async))
            {
                var a = new User { Email = "a@test" };
                db.Users.Add(a);
                db.Outbox.AddEvent(new UserCreated { User = a });
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);

                var b = new User { Email = "b@test" };
                db.Users.Add(b);
                db.Outbox.AddEvent(new UserCreated { User = b });
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: true);

                if (async) await tx.RollbackAsync(); else tx.Rollback();
            }

            Assert.Empty(db.Outbox.GetEvents());
            db.VersionedItems.Add(new Versioned { Name = "later" });
            await Save.Invoke(db, async);
            Assert.Equal(0, host.Count("OutboxMessages"));
            Assert.Empty(host.Queue.Enqueued);
        }
        finally
        {
            await lease.End(async);
        }
    }

    [Theory]
    [MemberData(nameof(RegistrationsAndAsync))]
    public async Task ReusedContext_SavepointRollbackOfFlagFalseSave_SaveAgainAndCommitDispatchesOnce(ContextRegistration registration, bool async)
    {
        using var host = PoolHost.Create(registration);
        var lease = await WarmLease(host);
        try
        {
            var db = lease.Db;
            var user = new User { Email = "sp@test" };
            var evt = new UserCreated { User = user };
            db.Users.Add(user);
            db.Outbox.AddEvent(evt);
            using (var tx = await PoolReuseAdversarialTests.Begin(db, async))
            {
                if (async) await tx.CreateSavepointAsync("before"); else tx.CreateSavepoint("before");
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                if (async) await tx.RollbackToSavepointAsync("before"); else tx.RollbackToSavepoint("before");
                Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));

                await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                if (async) await tx.CommitAsync(); else tx.Commit();
            }
            db.ChangeTracker.AcceptAllChanges();

            var row = Assert.Single(host.OutboxRows());
            Assert.Equal(user.Id, Payload.User(row.Payload).GetProperty("id").GetInt32());
            Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
            Assert.Single(host.Users());
            Assert.Empty(db.Outbox.GetEvents());
        }
        finally
        {
            await lease.End(async);
        }
    }

    [Theory]
    [MemberData(nameof(RegistrationsAndAsync))]
    public async Task ReusedContext_OwnedFailureThenAppRetry_RuleAPayloadKeysAndSingleDispatch(ContextRegistration registration, bool async)
    {
        using var host = PoolHost.Create(registration);
        var lease = await WarmLease(host);
        try
        {
            var db = lease.Db;
            var user = new User { Email = "retry@test", Orders = { new Order { Description = "o1" } } };
            db.Users.Add(user);
            db.Outbox.AddEvent(new UserCreated { User = user });

            host.Faults.FailCommands(SqlMatch.IsOutboxUpdate);
            await Assert.ThrowsAnyAsync<Exception>(() => Save.Invoke(db, async));
            host.Faults.Clear();
            Assert.Equal(1, host.Faults.CommandFailuresFired);
            Assert.Single(db.Outbox.GetEvents());
            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, host.Count("Users"));

            await Save.Invoke(db, async);

            var (dbId, _) = Assert.Single(host.Users());
            Assert.Equal(dbId, user.Id);
            var row = Assert.Single(host.OutboxRows());
            Assert.Equal(dbId, Payload.User(row.Payload).GetProperty("id").GetInt32());
            Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
            Assert.Equal(1, host.Count("Orders"));
        }
        finally
        {
            await lease.End(async);
        }
    }
}
