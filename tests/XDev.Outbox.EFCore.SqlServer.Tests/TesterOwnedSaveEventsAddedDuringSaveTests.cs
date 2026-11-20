using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Tester (adversarial) cases for fix-owned-save-drops-events-added-during-save on SQL Server: a retried owned save
/// whose first attempt fails after the database generated identity values (dependent Orders INSERT executed, or the
/// outbox payload UPDATE executed). A guarded SavingChanges handler collects one OrderPlaced per new order on the first
/// attempt only. The collected events are not part of the save (kept once, not once per attempt), and the next save
/// writes each once with the order ids of the committed attempt (not the discarded first-attempt ids).
/// </summary>
public class TesterOwnedSaveEventsAddedDuringSaveTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public TesterOwnedSaveEventsAddedDuringSaveTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    public enum AttemptFailure
    {
        DependentInsertAfterExecution,
        PayloadUpdateAfterExecution,
    }

    public static TheoryData<AttemptFailure, bool, bool> Cases()
    {
        var data = new TheoryData<AttemptFailure, bool, bool>();
        foreach (var failure in new[] { AttemptFailure.DependentInsertAfterExecution, AttemptFailure.PayloadUpdateAfterExecution })
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(failure, async, flag);
        return data;
    }

    [SqlServerTheory]
    [MemberData(nameof(Cases))]
    public async Task RetriedOwnedSave_FailureAfterIdentityGenerated_CollectedEventsKeptOnce_NextSaveWritesFinalKeys(AttemptFailure failure, bool async, bool flag)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RetryingStrategy = true });
        await using var scope = host.CreateScope();
        var db = scope.Db;
        var raised = new HashSet<Order>(ReferenceEqualityComparer.Instance);
        var handlerCalls = 0;
        db.SavingChanges += (_, _) =>
        {
            handlerCalls++;
            foreach (var entry in db.ChangeTracker.Entries<Order>().Where(e => e.State == EntityState.Added).ToList())
            {
                if (raised.Add(entry.Entity)) db.Outbox.AddEvent(new OrderPlaced { Order = entry.Entity });
            }
        };
        var user = Graph.NewUser("tester-retry@test", "t1", "t2");
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        if (failure == AttemptFailure.DependentInsertAfterExecution)
            host.Faults.FailCommands(sql => SqlMatch.WritesTo(sql, "Orders"), FaultTiming.AfterExecution);
        else
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);

        if (async) await db.SaveChangesAsync(flag, CancellationToken.None); else db.SaveChanges(flag);
        if (!flag) db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(1, host.Faults.CommandFailuresFired);
        Assert.Equal(2, handlerCalls);
        Assert.Equal(2, raised.Count);
        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var dbUser = Assert.Single(users);
        Assert.Equal(2, orders.Count);
        var first = Assert.Single(host.OutboxRows());
        GraphAssert.UserPayloadMatches(first.Payload, users, orders);
        Assert.Equal(2, db.Outbox.GetEvents().Count);
        Assert.All(db.Outbox.GetEvents(), e => Assert.IsType<OrderPlaced>(e));

        await Save.Invoke(db, async);

        var rows = host.OutboxRows();
        Assert.Equal(3, rows.Count);
        var orderIdsInPayloads = rows.Where(r => r.Type == typeof(OrderPlaced).AssemblyQualifiedName)
            .Select(r => Json.Parse(r.Payload!).GetProperty("order").GetProperty("id").GetInt32()).OrderBy(x => x);
        Assert.Equal(orders.Select(o => o.Id).OrderBy(x => x), orderIdsInPayloads);
        Assert.All(rows.Where(r => r.Type == typeof(OrderPlaced).AssemblyQualifiedName),
            r => Assert.Equal(dbUser.Id, Json.Parse(r.Payload!).GetProperty("order").GetProperty("userId").GetInt32()));
        var enqueued = host.Queue.Enqueued;
        Assert.Equal(rows.Select(r => r.Id), enqueued.Select(e => e.Id.ToString()));
        Assert.All(enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
        Assert.Empty(db.Outbox.GetEvents());

        db.Users.Add(Graph.NewUser("tester-unrelated@test"));
        await Save.Invoke(db, async);
        Assert.Equal(3, host.OutboxRows().Count);
        Assert.Equal(3, host.Queue.Enqueued.Count);
    }
}
