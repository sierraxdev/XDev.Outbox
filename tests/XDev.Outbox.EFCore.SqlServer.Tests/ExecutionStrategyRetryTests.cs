using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// SqlServerRetryingExecutionStrategy (EnableRetryOnFailure) re-runs the library-owned save after a transient failure.
/// "AfterExecution" failures let the statement run on the server first, so the failed attempt consumed identity
/// values and wrote rows that its rollback must remove.
/// </summary>
public class ExecutionStrategyRetryTests : IClassFixture<SmokeDatabaseFixture>
{
    private static readonly Regex ExplicitIdentityInsert = new(@"INSERT\s+INTO\s+\[(Users|Orders|OutboxMessages)\]\s*\([^)]*\[Id\]", RegexOptions.IgnoreCase);

    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ExecutionStrategyRetryTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public enum StrategyFailure
    {
        UserInsertBeforeExecution,
        UserInsertAfterExecution,
        OutboxInsertBeforeExecution,
        OutboxInsertAfterExecution,
        DependentInsertAfterExecution,
        PayloadUpdateBeforeExecution,
        PayloadUpdateAfterExecution,
        Commit,
    }

    public static TheoryData<StrategyFailure, bool> Cases()
    {
        var data = new TheoryData<StrategyFailure, bool>();
        foreach (var failure in Enum.GetValues<StrategyFailure>())
        foreach (var async in new[] { false, true })
            data.Add(failure, async);
        return data;
    }

    [SqlServerTheory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_WhenStepFailsOnceUnderSqlServerRetryingStrategy_RetryPersistsExactlyOneSetOfRowsWithFinalIdsAndDispatchesOnce(StrategyFailure failure, bool async)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RetryingStrategy = true });
        await using var scope = host.CreateScope();
        Assert.IsType<SmokeRetryingExecutionStrategy>(scope.Db.Database.CreateExecutionStrategy());

        var user = Graph.NewUser("strategy@test", "s1", "s2");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(new UserCreated { User = user }, new OrderPlaced { Order = user.Orders[1] });

        switch (failure)
        {
            case StrategyFailure.UserInsertBeforeExecution: host.Faults.FailCommands(SqlMatch.IsUserInsert, FaultTiming.BeforeExecution); break;
            case StrategyFailure.UserInsertAfterExecution: host.Faults.FailCommands(SqlMatch.IsUserInsert, FaultTiming.AfterExecution); break;
            case StrategyFailure.OutboxInsertBeforeExecution: host.Faults.FailCommands(SqlMatch.IsOutboxInsert, FaultTiming.BeforeExecution); break;
            case StrategyFailure.OutboxInsertAfterExecution: host.Faults.FailCommands(SqlMatch.IsOutboxInsert, FaultTiming.AfterExecution); break;
            case StrategyFailure.DependentInsertAfterExecution: host.Faults.FailCommands(sql => SqlMatch.WritesTo(sql, "Orders"), FaultTiming.AfterExecution); break;
            case StrategyFailure.PayloadUpdateBeforeExecution: host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.BeforeExecution); break;
            case StrategyFailure.PayloadUpdateAfterExecution: host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution); break;
            case StrategyFailure.Commit: host.Faults.FailCommits(); break;
        }

        await Save.Invoke(scope.Db, async);

        var fired = failure == StrategyFailure.Commit ? host.Faults.CommitFailuresFired : host.Faults.CommandFailuresFired;
        Assert.True(fired == 1, $"Precondition: the injected {failure} failure must fire exactly once (fired {fired}).");

        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var dbUser = Assert.Single(users);
        Assert.Equal(2, orders.Count);
        Assert.All(orders, o => Assert.Equal(dbUser.Id, o.UserId));
        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        GraphAssert.RowsMatch(rows, users, orders);

        var enqueued = host.Queue.Enqueued;
        Assert.Equal(rows.Select(r => r.Id), enqueued.Select(e => e.Id.ToString()));
        Assert.All(enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));

        Assert.Equal(dbUser.Id, user.Id);
        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        Assert.All(user.Orders, o => Assert.Equal(EntityState.Unchanged, scope.Db.Entry(o).State));
        Assert.Empty(scope.Db.Outbox.GetEvents());
        Assert.Empty(scope.Db.ChangeTracker.Entries<OutboxMessage<int>>());

        // The retried INSERTs never carry an explicit identity value (that would fail on SQL Server anyway).
        Assert.DoesNotContain(host.Faults.Commands, c => ExplicitIdentityInsert.IsMatch(c.Sql) || c.Sql.Contains("IDENTITY_INSERT", StringComparison.OrdinalIgnoreCase));

        var writes = host.Faults.Commands.Where(c => c.Sql.Contains("INSERT", StringComparison.OrdinalIgnoreCase) || c.Sql.Contains("MERGE", StringComparison.OrdinalIgnoreCase)).ToList();
        _output.WriteLine($"{failure}: user {dbUser.Id}, orders {string.Join(",", orders.Select(o => o.Id))}, outbox {string.Join(",", rows.Select(r => r.Id))}; write commands sent: {writes.Count}");
        foreach (var write in writes) _output.WriteLine("---- " + string.Join(" | ", write.Sql.Split('\n').Select(l => l.Trim()).Where(l => l.StartsWith("INSERT") || l.StartsWith("MERGE"))));
    }
}
