using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Saves inside a caller-owned transaction on SQL Server: dispatch on commit only, rollback rules (B1), several saves
/// per transaction, EF's automatic savepoints (SAVE TRANSACTION) around each save, and caller savepoints with the
/// library's existence query.
/// </summary>
public class CallerTransactionTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public CallerTransactionTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost() => SmokeHost<int, SmokeDbContext>.Create(_fixture);

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static async Task Rollback(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.RollbackAsync(); else tx.Rollback();
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerTransaction_SaveThenCommit_DispatchesOnlyAfterCommitWithGeneratedIds(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("caller@test", "c1", "c2");

        await using (var tx = await Begin(scope.Db, async))
        {
            scope.Db.Users.Add(user);
            scope.Db.Outbox.AddEvent(new UserCreated { User = user }, new OrderPlaced { Order = user.Orders[0] });
            await Save.Invoke(scope.Db, async);

            Assert.Empty(host.Queue.Enqueued);
            await Commit(tx, async);
        }

        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var rows = host.OutboxRows();
        Assert.Single(users);
        Assert.Equal(2, orders.Count);
        Assert.Equal(2, rows.Count);
        GraphAssert.RowsMatch(rows, users, orders);
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id.ToString()));
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
        Assert.Empty(scope.Db.Outbox.GetEvents());
        Assert.Contains("CreatedSavepoint", host.Faults.TransactionEvents);
        _output.WriteLine("Transaction events: " + string.Join(", ", host.Faults.TransactionEvents));
    }

    [SqlServerTheory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task CallerTransaction_RollbackAfterSave_NothingPersistedOrDispatchedAndBufferFollowsTheFlag(bool accept, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("rollback@test", "r1");

        await using (var tx = await Begin(scope.Db, async))
        {
            scope.Db.Users.Add(user);
            scope.Db.Outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(scope.Db, async ? (accept ? SaveCall.SaveChangesAsyncTrue : SaveCall.SaveChangesAsyncFalse) : (accept ? SaveCall.SaveChangesTrue : SaveCall.SaveChangesFalse));
            await Rollback(tx, async);
        }

        Assert.Empty(host.Database.Users());
        Assert.Empty(host.Database.Orders());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(accept ? 0 : 1, scope.Db.Outbox.GetEvents().Count);
        Assert.Equal(accept ? EntityState.Unchanged : EntityState.Added, scope.Db.Entry(user).State);
    }

    /// <summary>
    /// After a flag=false save and a rollback the business changes stay pending (B1), but EF keeps the identity
    /// values the rolled-back INSERT generated. Saving again is what an application would do. The plain-EF control
    /// (no events) shows whether any failure is EF's own behavior.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CallerTransaction_RollbackAfterFlagFalseSave_SavingAgainPersistsEverythingOnce(bool withEvents, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("again@test", "g1");

        await using (var tx = await Begin(scope.Db, async))
        {
            scope.Db.Users.Add(user);
            if (withEvents) scope.Db.Outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(scope.Db, async ? SaveCall.SaveChangesAsyncFalse : SaveCall.SaveChangesFalse);
            await Rollback(tx, async);
        }

        var idProperty = scope.Db.Entry(user).Property(x => x.Id);
        _output.WriteLine($"After rollback: state={scope.Db.Entry(user).State}, Id current value={idProperty.CurrentValue}, IsTemporary={idProperty.IsTemporary}, CLR Id={user.Id}");

        var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));
        for (var e = error; e != null; e = e.InnerException) _output.WriteLine($"{e.GetType().FullName}: {e.Message}");
        foreach (var c in host.Faults.Commands.Where(c => c.Sql.Contains("INSERT")).TakeLast(2)) _output.WriteLine(c.ToString());

        Assert.Null(error);
        var users = host.Database.Users();
        var orders = host.Database.Orders();
        Assert.Single(users);
        Assert.Single(orders);
        if (withEvents)
        {
            var row = Assert.Single(host.OutboxRows());
            GraphAssert.UserPayloadMatches(row.Payload, users, orders);
            Assert.Single(host.Queue.Enqueued);
        }
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerTransaction_TwoSavesThenCommit_DispatchesBothAfterCommitWithTheirIds(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();

        await using (var tx = await Begin(scope.Db, async))
        {
            var first = Graph.NewUser("first@test", "f1");
            scope.Db.Users.Add(first);
            scope.Db.Outbox.AddEvent(new UserCreated { User = first });
            await Save.Invoke(scope.Db, async);

            var second = Graph.NewUser("second@test", "s1");
            scope.Db.Users.Add(second);
            scope.Db.Outbox.AddEvent(new UserCreated { User = second }, new OrderPlaced { Order = second.Orders[0] });
            await Save.Invoke(scope.Db, async);

            Assert.Empty(host.Queue.Enqueued);
            await Commit(tx, async);
        }

        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var rows = host.OutboxRows();
        Assert.Equal(2, users.Count);
        Assert.Equal(3, rows.Count);
        GraphAssert.RowsMatch(rows, users, orders);
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id.ToString()));
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
        Assert.Empty(scope.Db.Outbox.GetEvents());
    }

    public enum SecondSaveFailure
    {
        UserAndOutboxBatchAfterExecution,
        DependentInsertAfterExecution,
    }

    /// <summary>
    /// EF wraps each save in a caller transaction in a savepoint (SAVE TRANSACTION on SQL Server). A failure after the
    /// INSERTs ran makes EF roll back to it; the library restores the tracker, so retrying the save inside the same
    /// transaction must persist everything once.
    /// </summary>
    [SqlServerTheory]
    [InlineData(SecondSaveFailure.UserAndOutboxBatchAfterExecution, false)]
    [InlineData(SecondSaveFailure.UserAndOutboxBatchAfterExecution, true)]
    [InlineData(SecondSaveFailure.DependentInsertAfterExecution, false)]
    [InlineData(SecondSaveFailure.DependentInsertAfterExecution, true)]
    public async Task CallerTransaction_SecondSaveFailsAfterItsInsertsRan_RetryInTheSameTransactionPersistsEverythingOnce(SecondSaveFailure failure, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();

        await using (var tx = await Begin(scope.Db, async))
        {
            var first = Graph.NewUser("sp-first@test", "sf1");
            scope.Db.Users.Add(first);
            scope.Db.Outbox.AddEvent(new UserCreated { User = first });
            await Save.Invoke(scope.Db, async);

            var second = Graph.NewUser("sp-second@test", "ss1");
            scope.Db.Users.Add(second);
            scope.Db.Outbox.AddEvent(new UserCreated { User = second }, new OrderPlaced { Order = second.Orders[0] });
            host.Faults.FailCommands(
                failure == SecondSaveFailure.DependentInsertAfterExecution ? sql => SqlMatch.WritesTo(sql, "Orders") : SqlMatch.IsUserInsert,
                FaultTiming.AfterExecution);

            var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));
            Assert.NotNull(error);
            Assert.Equal(1, host.Faults.CommandFailuresFired);
            Assert.Contains("RolledBackToSavepoint", host.Faults.TransactionEvents);
            Assert.Equal(EntityState.Added, scope.Db.Entry(second).State);
            Assert.Equal(3, scope.Db.Outbox.GetEvents().Count); // the first save's event stays buffered until commit

            await Save.Invoke(scope.Db, async);
            await Commit(tx, async);
        }

        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var rows = host.OutboxRows();
        Assert.Equal(2, users.Count);
        Assert.Equal(2, orders.Count);
        Assert.Equal(3, rows.Count);
        GraphAssert.RowsMatch(rows, users, orders);
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id.ToString()));
        _output.WriteLine("Transaction events: " + string.Join(", ", host.Faults.TransactionEvents));
    }

    /// <summary>
    /// D5 (known, deferred): the payload UPDATE runs after EF released its savepoint, so when it fails inside a caller
    /// transaction the INSERTs stay in the transaction while the tracker is restored to Added; retrying in the same
    /// transaction duplicates the business rows. Asserts the correct behavior; skipped while D5 is deferred.
    /// </summary>
    [SqlServerTheory(Skip = "D5 deferred (known limitation). Reproduced on SQL Server (net8.0-net11.0), both timings: after the caller's retry and commit there are 3 Users rows instead of 2 and 3 outbox rows instead of 2 (the failed attempt's row keeps the default-key payload and is never dispatched in memory; the poller would deliver it).")]
    [InlineData(FaultTiming.BeforeExecution, false)]
    [InlineData(FaultTiming.BeforeExecution, true)]
    [InlineData(FaultTiming.AfterExecution, false)]
    [InlineData(FaultTiming.AfterExecution, true)]
    public async Task CallerTransaction_SecondSavePayloadUpdateFails_RetryInTheSameTransactionDoesNotDuplicateRows(FaultTiming timing, bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();

        await using (var tx = await Begin(scope.Db, async))
        {
            var first = Graph.NewUser("d5-first@test");
            scope.Db.Users.Add(first);
            scope.Db.Outbox.AddEvent(new UserCreated { User = first });
            await Save.Invoke(scope.Db, async);

            var second = Graph.NewUser("d5-second@test");
            scope.Db.Users.Add(second);
            scope.Db.Outbox.AddEvent(new UserCreated { User = second });
            host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, timing);

            var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));
            Assert.NotNull(error);
            Assert.Equal(1, host.Faults.CommandFailuresFired);

            await Save.Invoke(scope.Db, async);
            await Commit(tx, async);
        }

        var users = host.Database.Users();
        var rows = host.OutboxRows();
        _output.WriteLine($"Users: {string.Join(", ", users)}; outbox rows: {rows.Count}; enqueued: {host.Queue.Enqueued.Count}");
        Assert.Equal(2, users.Count);
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, host.Queue.Enqueued.Count);
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerTransaction_RollbackToCallerSavepoint_RowsUndoneAreNotDispatchedAndEarlierRowsAre(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        int existenceQueries;

        await using (var tx = await Begin(scope.Db, async))
        {
            var kept = Graph.NewUser("kept@test", "k1");
            scope.Db.Users.Add(kept);
            scope.Db.Outbox.AddEvent(new UserCreated { User = kept });
            await Save.Invoke(scope.Db, async);

            if (async) await tx.CreateSavepointAsync("before_second"); else tx.CreateSavepoint("before_second");

            var undone = Graph.NewUser("undone@test", "u1");
            scope.Db.Users.Add(undone);
            scope.Db.Outbox.AddEvent(new UserCreated { User = undone }, new OrderPlaced { Order = undone.Orders[0] });
            await Save.Invoke(scope.Db, async);

            var before = host.Faults.Commands.Count;
            if (async) await tx.RollbackToSavepointAsync("before_second"); else tx.RollbackToSavepoint("before_second");
            var check = host.Faults.Commands.Skip(before).ToList();
            existenceQueries = check.Count(c => c.Sql.Contains("SELECT", StringComparison.OrdinalIgnoreCase) && c.Sql.Contains("[OutboxMessages]"));
            foreach (var c in check) _output.WriteLine("Existence query: " + c);

            await Commit(tx, async);
        }

        Assert.Equal(1, existenceQueries);
        var users = host.Database.Users();
        var orders = host.Database.Orders();
        var rows = host.OutboxRows();
        Assert.Equal("kept@test", Assert.Single(users).Email);
        Assert.Single(orders);
        var row = Assert.Single(rows);
        GraphAssert.UserPayloadMatches(row.Payload, users, orders);
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id.ToString());
        Assert.Equal("committed", enqueued.RowStateAtEnqueue);
        Assert.Empty(scope.Db.Outbox.GetEvents()); // flag=true: the undone events are dropped (B1)
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerTransaction_RollbackToSavepointWith151PendingMessages_ExistenceQueryRunsInBatchesAndKeepsTheSurvivors(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        List<CommandRecord> check;

        await using (var tx = await Begin(scope.Db, async))
        {
            for (var i = 0; i < 150; i++) scope.Db.Outbox.AddEvent(new TextEvent { Text = "kept-" + i });
            await Save.Invoke(scope.Db, async);

            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "undone" });
            await Save.Invoke(scope.Db, async);

            var before = host.Faults.Commands.Count;
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
            check = host.Faults.Commands.Skip(before).ToList();
            await Commit(tx, async);
        }

        _output.WriteLine($"Existence queries: {check.Count}; parameters: {string.Join(", ", check.Select(c => c.Parameters.Count))}");
        Assert.Equal(new[] { 100, 51 }, check.Select(c => c.Parameters.Count));
        var rows = host.OutboxRows();
        Assert.Equal(150, rows.Count);
        Assert.DoesNotContain(rows, r => r.Payload!.Contains("undone"));
        Assert.Equal(rows.Select(r => r.Id).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id.ToString()).OrderBy(x => x));
    }
}
