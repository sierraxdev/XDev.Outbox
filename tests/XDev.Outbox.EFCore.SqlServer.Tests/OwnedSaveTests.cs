using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Library-owned saves on SQL Server (no caller transaction): every public overload, application-level retry after a
/// failed attempt, a large batch, and an event type name longer than the Type column.
/// </summary>
public class OwnedSaveTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public OwnedSaveTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost() => SmokeHost<int, SmokeDbContext>.Create(_fixture);

    public static TheoryData<SaveCall> AllSaveCalls()
    {
        var data = new TheoryData<SaveCall>();
        foreach (var call in Enum.GetValues<SaveCall>()) data.Add(call);
        return data;
    }

    [SqlServerTheory]
    [MemberData(nameof(AllSaveCalls))]
    public async Task OwnedSave_PrincipalWithDependentsAndIdentityKeys_PersistsOneRowPerEventWithGeneratedIdsAndDispatchesCommittedRows(SaveCall call)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("owner@test", "first", "second");
        scope.Db.Users.Add(user);
        var userCreated = new UserCreated { User = user };
        var orderPlaced = new OrderPlaced { Order = user.Orders[1] };
        scope.Db.Outbox.AddEvent(userCreated, orderPlaced);

        var result = await Save.Invoke(scope.Db, call);

        Assert.Equal(3, result); // business rows only, as plain EF reports them
        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var dbUser = Assert.Single(users);
        Assert.Equal(2, orders.Count);
        Assert.All(orders, o => Assert.Equal(dbUser.Id, o.UserId));

        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        Assert.Equal(typeof(UserCreated).AssemblyQualifiedName, rows[0].Type);
        Assert.Equal(typeof(OrderPlaced).AssemblyQualifiedName, rows[1].Type);
        GraphAssert.UserPayloadMatches(rows[0].Payload, users, orders);
        GraphAssert.OrderPayloadMatches(rows[1].Payload, orders);
        Assert.Contains("\"orders\"", rows[0].Payload); // the dependents are in the payload too

        // Dispatched once per row, and each row was already committed when it was enqueued.
        var enqueued = host.Queue.Enqueued;
        Assert.Equal(rows.Select(r => r.Id), enqueued.Select(e => e.Id.ToString()));
        Assert.All(enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));

        if (Save.Accepts(call))
        {
            Assert.Same(userCreated, enqueued[0].Payload);
            Assert.Same(orderPlaced, enqueued[1].Payload);
            Assert.Equal(dbUser.Id, user.Id);
            Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        }
        else
        {
            // D3: the persisted JSON is dispatched; the tracker keeps the changes pending.
            Assert.True(GraphAssert.JsonEquals(rows[0].Payload, Json.Serialize(enqueued[0].Payload)), "Dispatched payload differs from the persisted one.");
            Assert.True(GraphAssert.JsonEquals(rows[1].Payload, Json.Serialize(enqueued[1].Payload)), "Dispatched payload differs from the persisted one.");
            Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
        }

        Assert.Empty(scope.Db.Outbox.GetEvents());
        Assert.Empty(scope.Db.ChangeTracker.Entries<OutboxMessage<int>>());
        Assert.Contains(host.Faults.Commands, c => SqlMatch.IsOutboxPayloadUpdate(c.Sql));
    }

    public enum AppRetryFailure
    {
        UserInsertAfterExecution,
        OutboxInsertAfterExecution,
        DependentInsertAfterExecution,
        PayloadUpdateAfterExecution,
        Commit,
    }

    public static TheoryData<AppRetryFailure, bool> AppRetryCases()
    {
        var data = new TheoryData<AppRetryFailure, bool>();
        foreach (var failure in Enum.GetValues<AppRetryFailure>())
        foreach (var async in new[] { false, true })
            data.Add(failure, async);
        return data;
    }

    [SqlServerTheory]
    [MemberData(nameof(AppRetryCases))]
    public async Task OwnedSave_WhenAttemptFailsWithoutRetryingStrategy_NothingIsCommittedAndTheSecondSavePersistsEverythingOnceWithFinalIds(AppRetryFailure failure, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("retry@test", "a", "b");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(new UserCreated { User = user }, new OrderPlaced { Order = user.Orders[0] });

        switch (failure)
        {
            case AppRetryFailure.UserInsertAfterExecution: host.Faults.FailCommands(SqlMatch.IsUserInsert, FaultTiming.AfterExecution); break;
            case AppRetryFailure.OutboxInsertAfterExecution: host.Faults.FailCommands(SqlMatch.IsOutboxInsert, FaultTiming.AfterExecution); break;
            case AppRetryFailure.DependentInsertAfterExecution: host.Faults.FailCommands(sql => SqlMatch.WritesTo(sql, "Orders"), FaultTiming.AfterExecution); break;
            case AppRetryFailure.PayloadUpdateAfterExecution: host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution); break;
            case AppRetryFailure.Commit: host.Faults.FailCommits(); break;
        }

        var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));

        Assert.NotNull(error);
        Assert.Equal(1, failure == AppRetryFailure.Commit ? host.Faults.CommitFailuresFired : host.Faults.CommandFailuresFired);
        Assert.Empty(host.Database.Users());
        Assert.Empty(host.Database.Orders());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(2, scope.Db.Outbox.GetEvents().Count);
        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
        Assert.Empty(scope.Db.ChangeTracker.Entries<OutboxMessage<int>>());

        await Save.Invoke(scope.Db, async);

        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var dbUser = Assert.Single(users);
        Assert.Equal(2, orders.Count);
        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        GraphAssert.RowsMatch(rows, users, orders);
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id.ToString()));
        Assert.Equal(dbUser.Id, user.Id);
        Assert.Empty(scope.Db.Outbox.GetEvents());
        _output.WriteLine($"{failure}: final user id {dbUser.Id}, order ids {string.Join(",", orders.Select(o => o.Id))}, outbox ids {string.Join(",", rows.Select(r => r.Id))}");
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSave_With200EntitiesAnd200Events_PersistsEveryRowWithItsOwnGeneratedIds(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        for (var i = 0; i < 100; i++)
        {
            var user = Graph.NewUser($"bulk{i}@test", $"bulk-order-{i}");
            scope.Db.Users.Add(user);
            scope.Db.Outbox.AddEvent(new UserCreated { User = user }, new OrderPlaced { Order = user.Orders[0] });
        }

        var result = await Save.Invoke(scope.Db, async);

        Assert.Equal(200, result);
        var users = host.Database.Users();
        var orders = host.Database.Orders();
        Assert.Equal(100, users.Count);
        Assert.Equal(100, orders.Count);
        var rows = host.OutboxRows();
        Assert.Equal(200, rows.Count);
        GraphAssert.RowsMatch(rows, users, orders);
        Assert.Equal(rows.Select(r => r.Id).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id.ToString()).OrderBy(x => x));
        Assert.Equal(200, host.Queue.Enqueued.Select(e => e.Id).Distinct().Count());

        var commands = host.Faults.Commands;
        var updates = commands.Count(c => SqlMatch.IsOutboxPayloadUpdate(c.Sql));
        _output.WriteLine($"Commands: {commands.Count} total; INSERT/MERGE batches: {commands.Count(c => c.Sql.Contains("INSERT", StringComparison.OrdinalIgnoreCase))}; payload UPDATEs: {updates}");
        foreach (var command in commands.Where(c => !SqlMatch.IsOutboxPayloadUpdate(c.Sql)))
        {
            _output.WriteLine("---- " + command.Sql.Split('\n')[0].Trim() + $" ... ({command.Parameters.Count} parameters, {command.Sql.Length} chars)");
        }
        Assert.Equal(200, updates); // one UPDATE per message whose payload changed (documented behavior)
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSave_WhenEventTypeNameExceeds200Characters_WholeSaveFailsAndNothingIsPersisted(bool async)
    {
        var eventType = typeof(LongNamedEvent<Dictionary<string, List<KeyValuePair<int, Guid>>>>);
        Assert.True(eventType.AssemblyQualifiedName!.Length > 200, "Precondition: the type name must exceed the 200-character column.");

        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("long@test", "o");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(Activator.CreateInstance(eventType)!);

        var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));

        _output.WriteLine($"Type name length: {eventType.AssemblyQualifiedName.Length}");
        for (var e = error; e != null; e = e.InnerException) _output.WriteLine($"{e.GetType().FullName}: {e.Message}");
        Assert.NotNull(error);
        Assert.IsType<DbUpdateException>(error);
        var sql = Assert.IsType<Microsoft.Data.SqlClient.SqlException>(error!.InnerException);
        _output.WriteLine($"SqlException.Number = {sql.Number}");
        Assert.Empty(host.Database.Users());
        Assert.Empty(host.Database.Orders());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Single(scope.Db.Outbox.GetEvents());
        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);

        var typeParameter = host.Faults.Commands.Where(c => SqlMatch.IsOutboxInsert(c.Sql))
            .SelectMany(c => c.Parameters).First(p => p.ValueLength == eventType.AssemblyQualifiedName.Length);
        _output.WriteLine($"Type parameter: {typeParameter.SqlDbType} size={typeParameter.Size} valueLength={typeParameter.ValueLength}");
    }
}
