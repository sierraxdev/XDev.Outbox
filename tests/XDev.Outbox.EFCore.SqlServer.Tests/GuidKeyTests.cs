using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>Guid outbox keys: OutboxDbContext&lt;Guid&gt;, AddOutbox&lt;Guid&gt;, AddEFCoreOutboxStore&lt;Guid, TContext&gt;.</summary>
public class GuidKeyTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public GuidKeyTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<Guid, GuidDbContext> CreateHost() => SmokeHost<Guid, GuidDbContext>.Create(_fixture);

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSave_GuidKey_PersistsRowsWithGeneratedIdsInPayloadsAndDispatchesThem(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("guid@test", "g1", "g2");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(new UserCreated { User = user }, new OrderPlaced { Order = user.Orders[0] });

        await Save.Invoke(scope.Db, async);

        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.NotEqual(Guid.Empty, Guid.Parse(r.Id)));
        GraphAssert.RowsMatch(rows, users, orders);
        Assert.Equal(rows.Select(r => Guid.Parse(r.Id)).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id).OrderBy(x => x));
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));

        var updates = host.Faults.Commands.Where(c => SqlMatch.IsOutboxPayloadUpdate(c.Sql)).ToList();
        Assert.Equal(2, updates.Count);
        _output.WriteLine(updates[0].ToString());
        Assert.All(updates, u => Assert.Equal("UniqueIdentifier", u.Parameters[1].SqlDbType));
    }

    [SqlServerFact]
    public async Task Store_GuidKey_ClaimsUpdatesAndRemoves()
    {
        using var host = CreateHost();
        await using (var scope = host.CreateScope())
        {
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "one" }, new TextEvent { Text = "two" });
            await scope.Db.SaveChangesAsync();
        }
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = NULL");

        await using var claimScope = host.CreateScope();
        var store = claimScope.Services.GetRequiredService<IOutboxStore<Guid>>();
        var claimed = (await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15)).ToList();
        Assert.Equal(2, claimed.Count);
        Assert.Equal(host.OutboxRows().Select(r => Guid.Parse(r.Id)), claimed.Select(c => c.Id)); // both ordered by the server's uniqueidentifier order
        Assert.All(host.OutboxRows(), r => Assert.NotNull(r.LockExpiresAt));

        claimed[0].Attemps = 1;
        claimed[0].IsError = true;
        claimed[0].ErrorMessage = "failed";
        await store.UpdateAsync(claimed[0]);
        await store.RemoveAsync(claimed[1]);

        var remaining = Assert.Single(host.OutboxRows());
        Assert.Equal(claimed[0].Id, Guid.Parse(remaining.Id));
        Assert.Equal(1, remaining.Attemps);
        Assert.Equal("failed", remaining.ErrorMessage);
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerSavepointRollback_GuidKey_ExistenceQueryReleasesOnlyTheUndoneMessage(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        await using (var tx = async ? await scope.Db.Database.BeginTransactionAsync() : scope.Db.Database.BeginTransaction())
        {
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "kept" });
            await Save.Invoke(scope.Db, async);
            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "undone" });
            await Save.Invoke(scope.Db, async);
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.Contains("kept", row.Payload);
        Assert.Equal(Guid.Parse(row.Id), Assert.Single(host.Queue.Enqueued).Id);
    }
}
