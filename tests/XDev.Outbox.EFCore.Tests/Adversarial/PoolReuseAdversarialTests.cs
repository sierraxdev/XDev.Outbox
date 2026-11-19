using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>How the first lease of a pooled context ends before the instance is reused.</summary>
public enum FirstLeaseEnd
{
    /// <summary>Owned save fails at the outbox INSERT (D2: the buffer is kept).</summary>
    FailedOwnedSave,

    /// <summary>Caller transaction, flag=false save, caller rolls back (B1: the event stays buffered).</summary>
    CallerRollbackAfterAcceptFalse,

    /// <summary>Caller transaction, flag=false save, commit (dispatched), AcceptAllChanges never called.</summary>
    CommittedWithAcceptancePending,

    /// <summary>Rollback to a savepoint whose existence check fails: pending messages unconfirmed, transaction left open.</summary>
    SavepointRollbackUnconfirmed,

    /// <summary>Retrying strategy: every attempt fails, RetryLimitExceeded (buffer kept).</summary>
    RetriesExhausted,

    /// <summary>Retrying strategy: the token is cancelled while the strategy is retrying.</summary>
    RetryCancelled,

    /// <summary>Caller transaction: the save throws mid-way (outbox INSERT), transaction left open.</summary>
    AmbientSaveThrows,

    /// <summary>Caller transaction, flag=false save, transaction disposed without commit or rollback (no EF event).</summary>
    TransactionDisposedNotRolledBack,

    /// <summary>Caller transaction, flag=true save with event, scope ends with the transaction open.</summary>
    CallerTransactionLeftOpenAcceptTrue,
}

/// <summary>
/// Adversarial: a pooled context (poolSize 1, so the same instance is reused) must start every lease with an empty
/// buffer and no pending/transaction state, whatever state the previous lease left behind; nothing of the earlier
/// lease is persisted or dispatched later, and the new lease's own events work normally.
/// </summary>
public class PoolReuseAdversarialTests
{
    public static TheoryData<ContextRegistration, FirstLeaseEnd, bool> Cases()
    {
        var data = new TheoryData<ContextRegistration, FirstLeaseEnd, bool>();
        foreach (var registration in new[] { ContextRegistration.AddDbContextPool, ContextRegistration.PooledFactory, ContextRegistration.PooledFactoryWithScopedContext })
        foreach (var end in Enum.GetValues<FirstLeaseEnd>())
        foreach (var asyncDispose in new[] { false, true })
        {
            data.Add(registration, end, asyncDispose);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task PooledContext_AfterLeaseEndsInAnyState_NextLeaseIsCleanAndWorks(ContextRegistration registration, FirstLeaseEnd end, bool asyncDispose)
    {
        var retrying = end is FirstLeaseEnd.RetriesExhausted or FirstLeaseEnd.RetryCancelled;
        using var host = PoolHost.Create(registration, poolSize: 1, retrying: retrying);
        // Async work in the first lease alternates with the dispose mode so both sync and async saves are covered.
        var async = asyncDispose;

        var first = host.Lease();
        var instanceId = first.Db.ContextId.InstanceId;
        var firstLease = first.Db.ContextId.Lease;
        var expectedDispatched = await RunFirstLease(host, first.Db, end, async);
        await first.End(asyncDispose);

        // State the first lease leaves in the database.
        var usersAfterFirst = host.Count("Users");
        var rowsAfterFirst = host.OutboxRows();
        Assert.Equal(expectedDispatched, host.Queue.Enqueued.Count);
        Assert.Equal(expectedDispatched, rowsAfterFirst.Count);
        Assert.Equal(expectedDispatched, usersAfterFirst);

        var second = host.Lease();
        try
        {
            var db = second.Db;
            Assert.Equal(instanceId, db.ContextId.InstanceId);
            Assert.NotEqual(firstLease, db.ContextId.Lease);
            Assert.Empty(db.Outbox.GetEvents());
            Assert.Empty(db.ChangeTracker.Entries());
            Assert.Null(db.Database.CurrentTransaction);

            if (!retrying)
            {
                // Plain save in a caller transaction: nothing stale re-persisted or dispatched at commit.
                await using (var tx = await Begin(db, async))
                {
                    db.Users.Add(new User { Email = "plain-tx@test" });
                    await Save.Invoke(db, async);
                    if (async) await tx.CommitAsync(); else tx.Commit();
                }
            }

            // Plain owned save: no stale outbox row.
            db.VersionedItems.Add(new Versioned { Name = "plain-owned" });
            await Save.Invoke(db, async);
            Assert.Equal(rowsAfterFirst.Count, host.Count("OutboxMessages"));
            Assert.Equal(expectedDispatched, host.Queue.Enqueued.Count);

            // The new lease's own event, owned save.
            var mine = new User { Email = "mine@test" };
            db.Users.Add(mine);
            db.Outbox.AddEvent(new UserCreated { User = mine });
            await Save.Invoke(db, async);

            var newRows = host.OutboxRows().Skip(rowsAfterFirst.Count).ToList();
            var row = Assert.Single(newRows);
            Assert.Equal("mine@test", Payload.User(row.Payload).GetProperty("email").GetString());
            Assert.Equal(mine.Id, Payload.User(row.Payload).GetProperty("id").GetInt32());
            Assert.Equal(row.Id, host.Queue.Enqueued.Last().Id);
            Assert.Equal(expectedDispatched + 1, host.Queue.Enqueued.Count);
            Assert.Empty(db.Outbox.GetEvents());

            if (!retrying)
            {
                // The new lease's own event in a caller transaction, flag=false then commit and accept.
                var mine2 = new User { Email = "mine2@test" };
                await using (var tx = await Begin(db, async))
                {
                    db.Users.Add(mine2);
                    db.Outbox.AddEvent(new UserCreated { User = mine2 });
                    await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                    Assert.Equal(expectedDispatched + 1, host.Queue.Enqueued.Count);
                    if (async) await tx.CommitAsync(); else tx.Commit();
                }
                db.ChangeTracker.AcceptAllChanges();
                Assert.Equal(expectedDispatched + 2, host.Queue.Enqueued.Count);
                var last = host.OutboxRows().Last();
                Assert.Equal("mine2@test", Payload.User(last.Payload).GetProperty("email").GetString());
                Assert.Equal(last.Id, host.Queue.Enqueued.Last().Id);
            }
        }
        finally
        {
            await second.End(asyncDispose);
        }

        var expectedNewRows = retrying ? 1 : 2;
        Assert.Equal(rowsAfterFirst.Count + expectedNewRows, host.Count("OutboxMessages"));
        Assert.Equal(expectedDispatched + expectedNewRows, host.Queue.Enqueued.Count);
        // No row of the first lease's never-committed users ever appears.
        Assert.Equal(expectedDispatched, host.Users().Count(u => u.Email.StartsWith("first", StringComparison.Ordinal)));
        host.AssertEveryDispatchHasACommittedRow();

        // A third lease (the instance again) is clean as well.
        var third = host.Lease();
        try
        {
            Assert.Equal(instanceId, third.Db.ContextId.InstanceId);
            Assert.Empty(third.Db.Outbox.GetEvents());
            third.Db.VersionedItems.Add(new Versioned { Name = "third" });
            await Save.Invoke(third.Db, async);
        }
        finally
        {
            await third.End(asyncDispose);
        }
        Assert.Equal(rowsAfterFirst.Count + expectedNewRows, host.Count("OutboxMessages"));
        Assert.Equal(expectedDispatched + expectedNewRows, host.Queue.Enqueued.Count);
    }

    /// <summary>Runs the first lease; returns how many messages it legitimately committed and dispatched.</summary>
    private static async Task<int> RunFirstLease(PoolHost host, TestDbContext db, FirstLeaseEnd end, bool async)
    {
        var user = new User { Email = "first@test" };
        var evt = new UserCreated { User = user };
        switch (end)
        {
            case FirstLeaseEnd.FailedOwnedSave:
            {
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                host.Faults.FailCommands(SqlMatch.IsOutboxInsert);
                await Assert.ThrowsAnyAsync<Exception>(() => Save.Invoke(db, async));
                host.Faults.Clear();
                Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));
                return 0;
            }
            case FirstLeaseEnd.CallerRollbackAfterAcceptFalse:
            {
                using var tx = await Begin(db, async);
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                if (async) await tx.RollbackAsync(); else tx.Rollback();
                Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));
                Assert.Equal(EntityState.Added, db.Entry(user).State);
                return 0;
            }
            case FirstLeaseEnd.CommittedWithAcceptancePending:
            {
                using var tx = await Begin(db, async);
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                if (async) await tx.CommitAsync(); else tx.Commit();
                Assert.Single(host.Queue.Enqueued);
                Assert.Equal(EntityState.Added, db.Entry(user).State);
                // Another event added but never saved in this lease.
                db.Outbox.AddEvent(new UserCreated { User = new User { Email = "first-unsaved@test" } });
                return 1;
            }
            case FirstLeaseEnd.SavepointRollbackUnconfirmed:
            {
                var tx = await Begin(db, async);
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: true);
                if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
                var second = new User { Email = "first-2@test" };
                db.Users.Add(second);
                db.Outbox.AddEvent(new UserCreated { User = second });
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                host.Faults.FailCommands(sql => sql.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) && sql.Contains("OutboxMessages"));
                var error = await Record.ExceptionAsync(async () =>
                {
                    if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
                });
                host.Faults.Clear();
                Assert.NotNull(error);
                Assert.Equal(1, host.Faults.CommandFailuresFired);
                // The transaction is left open: the lease ends with unconfirmed pending messages.
                return 0;
            }
            case FirstLeaseEnd.RetriesExhausted:
            {
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                host.Faults.FailCommands(SqlMatch.IsOutboxInsert, times: 100);
                await Assert.ThrowsAnyAsync<Exception>(() => Save.Invoke(db, async));
                host.Faults.Clear();
                Assert.True(host.Faults.CommandFailuresFired > 1, "Precondition: the strategy retried.");
                Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));
                return 0;
            }
            case FirstLeaseEnd.RetryCancelled:
            {
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                using var cts = new CancellationTokenSource();
                host.Faults.FailCommands(sql =>
                {
                    if (!SqlMatch.IsOutboxInsert(sql)) return false;
                    cts.Cancel();
                    return true;
                }, times: 100);
                await Assert.ThrowsAnyAsync<Exception>(() => db.SaveChangesAsync(cts.Token));
                host.Faults.Clear();
                Assert.True(host.Faults.CommandFailuresFired >= 1);
                Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));
                return 0;
            }
            case FirstLeaseEnd.AmbientSaveThrows:
            {
                var tx = await Begin(db, async);
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                host.Faults.FailCommands(SqlMatch.IsOutboxInsert);
                await Assert.ThrowsAnyAsync<Exception>(() => Save.Invoke(db, async));
                host.Faults.Clear();
                Assert.Single(db.Outbox.GetEvents());
                return 0;
            }
            case FirstLeaseEnd.TransactionDisposedNotRolledBack:
            {
                var tx = await Begin(db, async);
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                if (async) await tx.DisposeAsync(); else tx.Dispose();
                return 0;
            }
            case FirstLeaseEnd.CallerTransactionLeftOpenAcceptTrue:
            {
                var tx = await Begin(db, async);
                db.Users.Add(user);
                db.Outbox.AddEvent(evt);
                await Save.Invoke(db, async, acceptAllChangesOnSuccess: true);
                return 0;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(end));
        }
    }

    internal static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();
}
