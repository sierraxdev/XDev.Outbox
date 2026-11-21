using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// fix-owned-save-drops-events-added-during-save on SQL Server: a SavingChanges handler that collects domain events
/// (one <see cref="OrderPlaced"/> per new order) during a library-owned save of a user + orders graph with identity
/// keys. Those events are not part of the save that raised them; they stay buffered and the next save writes each
/// once, with the keys the database generated, and dispatches each once after its commit. Before the fix the owned
/// save cleared the whole buffer and they were dropped without rows.
/// </summary>
public class OwnedSaveEventsAddedDuringSaveTests : IClassFixture<SmokeDatabaseFixture>
{
    private static readonly Regex ExplicitIdentityInsert = new(@"INSERT\s+INTO\s+\[(Users|Orders|OutboxMessages)\]\s*\([^)]*\[Id\]", RegexOptions.IgnoreCase);

    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public OwnedSaveEventsAddedDuringSaveTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public static TheoryData<bool, bool> AsyncFlag()
    {
        var data = new TheoryData<bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(async, flag);
        return data;
    }

    /// <summary>Raises one OrderPlaced per new order, once per order instance (guarded across attempts and saves).</summary>
    private static Func<int> CollectOrderPlacedOnSavingChanges(SmokeDbContext db)
    {
        var raised = new HashSet<Order>(ReferenceEqualityComparer.Instance);
        db.SavingChanges += (_, _) =>
        {
            foreach (var entry in db.ChangeTracker.Entries<Order>().Where(e => e.State == EntityState.Added).ToList())
            {
                if (raised.Add(entry.Entity)) db.Outbox.AddEvent(new OrderPlaced { Order = entry.Entity });
            }
        };
        return () => raised.Count;
    }

    private static async Task SaveAndAccept(DbContext db, bool async, bool flag)
    {
        if (async) await db.SaveChangesAsync(flag, CancellationToken.None); else db.SaveChanges(flag);
        if (!flag) db.ChangeTracker.AcceptAllChanges();
    }

    /// <summary>First save: the user and its explicit UserCreated committed; the two collected OrderPlaced kept. Next save: both written once with final keys; every row dispatched once, committed.</summary>
    private static async Task AssertCollectedEventsWrittenOnceByNextSave(SmokeHost<int, SmokeDbContext> host, SmokeDbContext db, User user, Func<int> raised, bool async)
    {
        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var dbUser = Assert.Single(users);
        Assert.Equal(2, orders.Count);
        Assert.All(orders, o => Assert.Equal(dbUser.Id, o.UserId));
        var first = Assert.Single(host.OutboxRows());
        Assert.Equal(StoredTypeName.Of<UserCreated>(), first.Type);
        GraphAssert.UserPayloadMatches(first.Payload, users, orders);
        Assert.Equal(2, raised());
        var kept = db.Outbox.GetEvents().ToList();
        Assert.Equal(2, kept.Count);
        Assert.All(kept, e => Assert.IsType<OrderPlaced>(e));
        Assert.Equal(dbUser.Id, user.Id);

        await Save.Invoke(db, async);

        Assert.Equal(2, raised());
        var rows = host.OutboxRows();
        Assert.Equal(3, rows.Count);
        Assert.Equal(2, rows.Count(r => r.Type == StoredTypeName.Of<OrderPlaced>()));
        GraphAssert.RowsMatch(rows, users, orders);
        var orderIdsInPayloads = rows.Where(r => r.Type == StoredTypeName.Of<OrderPlaced>())
            .Select(r => Json.Parse(r.Payload!).GetProperty("order").GetProperty("id").GetInt32()).OrderBy(x => x);
        Assert.Equal(orders.Select(o => o.Id).OrderBy(x => x), orderIdsInPayloads);

        var enqueued = host.Queue.Enqueued;
        Assert.Equal(rows.Select(r => r.Id), enqueued.Select(e => e.Id.ToString()));
        Assert.All(enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
        Assert.Empty(db.Outbox.GetEvents());
        Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());

        // Nothing more is written or dispatched by a later save.
        db.Users.Add(Graph.NewUser("unrelated@test"));
        await Save.Invoke(db, async);
        Assert.Equal(3, host.OutboxRows().Count);
        Assert.Equal(3, host.Queue.Enqueued.Count);
        Assert.Empty(db.Outbox.GetEvents());
    }

    [SqlServerTheory]
    [MemberData(nameof(AsyncFlag))]
    public async Task OwnedSave_SavingChangesHandlerCollectsDomainEvents_KeptAndWrittenOnceByNextSaveWithIdentityKeys(bool async, bool flag)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        await using var scope = host.CreateScope();
        var raised = CollectOrderPlacedOnSavingChanges(scope.Db);
        var user = Graph.NewUser("handler@test", "h1", "h2");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(new UserCreated { User = user });

        await SaveAndAccept(scope.Db, async, flag);

        await AssertCollectedEventsWrittenOnceByNextSave(host, scope.Db, user, raised, async);
    }

    [SqlServerTheory]
    [MemberData(nameof(AsyncFlag))]
    public async Task OwnedSave_RetryingStrategyCommitFailsOnce_HandlerEventsKeptOnce_WrittenOnceByNextSaveWithIdentityKeys(bool async, bool flag)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RetryingStrategy = true });
        await using var scope = host.CreateScope();
        Assert.IsType<SmokeRetryingExecutionStrategy>(scope.Db.Database.CreateExecutionStrategy());
        var raised = CollectOrderPlacedOnSavingChanges(scope.Db);
        var user = Graph.NewUser("retry-handler@test", "r1", "r2");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(new UserCreated { User = user });

        host.Faults.FailCommits();
        await SaveAndAccept(scope.Db, async, flag);

        Assert.Equal(1, host.Faults.CommitFailuresFired);
        await AssertCollectedEventsWrittenOnceByNextSave(host, scope.Db, user, raised, async);
        Assert.DoesNotContain(host.Faults.Commands, c => ExplicitIdentityInsert.IsMatch(c.Sql) || c.Sql.Contains("IDENTITY_INSERT", StringComparison.OrdinalIgnoreCase));
        _output.WriteLine($"outbox ids {string.Join(",", host.OutboxRows().Select(r => r.Id))}");
    }
}
