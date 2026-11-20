using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Adversarial (tester) checks of fix-commit-drops-never-saved-events on SQL Server (own throwaway database): a
/// commit keeps events never saved in it, with real identity keys (user and orders) and a doomed transaction. A
/// CHECK constraint rejects a payload UPDATE of a user whose e-mail contains "failupdate" (error 547).
/// </summary>
public class TesterNeverSavedOnCommitTests : IClassFixture<SmokeDatabaseFixture>
{
    private const int CheckViolation = 547;

    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterNeverSavedOnCommitTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost()
    {
        var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions());
        host.Database.Execute("""
            IF OBJECT_ID(N'[dbo].[CK_Outbox_FailUpdate]') IS NULL
                ALTER TABLE [dbo].[OutboxMessages] ADD CONSTRAINT [CK_Outbox_FailUpdate]
                    CHECK ([Payload] IS NULL OR [Payload] NOT LIKE N'%failupdate%' OR [Payload] LIKE N'%"id":0,%');
            """);
        return host;
    }

    private static async Task<IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    private static async Task Commit(IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    private static SaveCall Call(bool async, bool flag)
        => async
            ? (flag ? SaveCall.SaveChangesAsyncTrue : SaveCall.SaveChangesAsyncFalse)
            : (flag ? SaveCall.SaveChangesTrue : SaveCall.SaveChangesFalse);

    private static User AddUserWithEvent(SmokeDbContext db, string email, params string[] orders)
    {
        var user = Graph.NewUser(email, orders);
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });
        return user;
    }

    private static void AcceptUserGraph(DbContext db, User user)
    {
        db.Entry(user).State = EntityState.Unchanged;
        foreach (var order in user.Orders) db.Entry(order).State = EntityState.Unchanged;
    }

    private static void AssertCommittedAndDispatchedOnce(SmokeHost<int, SmokeDbContext> host, params string[] emails)
    {
        var users = host.Database.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        GraphAssert.RowsMatch(rows, users, host.Database.Orders());
        Assert.Equal(rows.Select(r => r.Id).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id.ToString()).OrderBy(x => x));
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
    }

    public enum Scenario
    {
        /// <summary>A save commits "saved" (with orders), then "never" (with orders) is added and the caller commits.</summary>
        AddedAfterLastSave,

        /// <summary>A save commits "saved", then a save of "never" fails after its payload UPDATE ran (undone by the library savepoint) and the caller commits.</summary>
        FailedSaveAfterSuccessfulSave,
    }

    /// <summary>
    /// The commit dispatches the saved event once (its row committed, real identity keys in the payload) and keeps
    /// the never-saved one; the next save commits "never" with its orders and its row once; nothing is rewritten.
    /// </summary>
    [SqlServerTheory]
    [InlineData(Scenario.AddedAfterLastSave, false, false)]
    [InlineData(Scenario.AddedAfterLastSave, false, true)]
    [InlineData(Scenario.AddedAfterLastSave, true, false)]
    [InlineData(Scenario.AddedAfterLastSave, true, true)]
    [InlineData(Scenario.FailedSaveAfterSuccessfulSave, false, false)]
    [InlineData(Scenario.FailedSaveAfterSuccessfulSave, false, true)]
    [InlineData(Scenario.FailedSaveAfterSuccessfulSave, true, false)]
    [InlineData(Scenario.FailedSaveAfterSuccessfulSave, true, true)]
    public async Task NeverSavedEventAfterSuccessfulSave_Commit_SavedDispatchedOnceNeverKeptAndWrittenOnce(Scenario scenario, bool async, bool flag)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;

        User saved, never;
        await using (var tx = await Begin(db, async))
        {
            saved = AddUserWithEvent(db, "saved@test", "s1", "s2");
            await Save.Invoke(db, Call(async, flag));
            if (scenario == Scenario.AddedAfterLastSave)
            {
                never = AddUserWithEvent(db, "never@test", "n1");
            }
            else
            {
                // With flag = false the saved user is still Added, so it is not part of this (failing) save only when
                // accepted first; the app accepts it here the way it would after its own logic.
                if (!flag) AcceptUserGraph(db, saved);
                never = AddUserWithEvent(db, "never@test", "n1");
                host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, FaultTiming.AfterExecution);
                Assert.IsType<InjectedTransientException>(await Record.ExceptionAsync(() => Save.Invoke(db, Call(async, flag))));
                Assert.Equal(EntityState.Added, db.Entry(never).State);
            }
            await Commit(tx, async);
        }

        AssertCommittedAndDispatchedOnce(host, "saved@test");
        Assert.Single(db.Outbox.GetEvents());

        if (!flag && scenario == Scenario.AddedAfterLastSave) AcceptUserGraph(db, saved);
        await Save.Invoke(db, async);

        AssertCommittedAndDispatchedOnce(host, "saved@test", "never@test");
        Assert.Equal(3, host.Database.Orders().Count);
        Assert.Empty(db.Outbox.GetEvents());

        // Nothing more is written by a later empty commit and a later save without events.
        await using (var tx = await Begin(db, async))
        {
            await Commit(tx, async);
        }
        db.Users.Add(Graph.NewUser("plain@test"));
        await Save.Invoke(db, async);
        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(2, host.Queue.Enqueued.Count);
    }

    /// <summary>
    /// XACT_ABORT ON: a successful save, then a save whose payload UPDATE violates the CHECK constraint dooms the
    /// transaction (the server rolls it back entirely; the library's rollback to its savepoint fails, warning 41002).
    /// The caller then calls Commit instead of Rollback. The commit must not succeed silently and must not dispatch
    /// anything (no phantom for the first save, whose row the server rolled back). The failed save's event stays
    /// buffered; the first event follows B1 (flag = false: kept with its Added user; flag = true: dropped, its user was
    /// accepted and is lost, as with plain EF). A new transaction persists what remains exactly once.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task DoomedTransaction_CallerCommitsInsteadOfRollingBack_NoPhantomDispatchAndRetryPersistsOnce(bool async, bool flag)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        var db = scope.Db;

        User ok, doomed;
        var tx = await Begin(db, async);
        try
        {
            if (async) await db.Database.ExecuteSqlRawAsync("SET XACT_ABORT ON"); else db.Database.ExecuteSqlRaw("SET XACT_ABORT ON");
            ok = AddUserWithEvent(db, "ok@test", "o1");
            await Save.Invoke(db, Call(async, flag));
            if (!flag) AcceptUserGraph(db, ok);

            doomed = AddUserWithEvent(db, "failupdate@test", "d1");
            var saveError = await Record.ExceptionAsync(() => Save.Invoke(db, Call(async, true)));
            Assert.Equal(CheckViolation, Assert.IsType<SqlException>(saveError).Number);

            var commitError = await Record.ExceptionAsync(() => Commit(tx, async));
            _output.WriteLine($"Commit: {commitError?.GetType().Name}: {commitError?.Message}");
            Assert.NotNull(commitError);
        }
        finally
        {
            if (async) await tx.DisposeAsync(); else tx.Dispose();
            if (async) await db.Database.ExecuteSqlRawAsync("SET XACT_ABORT OFF"); else db.Database.ExecuteSqlRaw("SET XACT_ABORT OFF");
        }

        Assert.Empty(host.Database.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        // The ok user was accepted by the test (flag = false) or by the save (flag = true); either way its message
        // was accepted only when flag = true. B1: kept when not accepted by the library.
        _output.WriteLine($"Buffered after doomed commit: {db.Outbox.GetEvents().Count}");
        Assert.Equal(flag ? 1 : 2, db.Outbox.GetEvents().Count);
        Assert.Equal(EntityState.Added, db.Entry(doomed).State);

        // The application makes the failing user valid and re-adds the user it had accepted (identity values are not
        // reused on SQL Server, so the accepted ghost does not conflict; it is detached and a fresh graph is used).
        doomed.Email = "fixed@test";
        db.Entry(ok).State = EntityState.Detached;
        foreach (var order in ok.Orders) db.Entry(order).State = EntityState.Detached;
        await using (var retry = await Begin(db, async))
        {
            if (!flag)
            {
                // The kept event references the detached ok user; the app re-adds that same graph as new.
                ok.Id = 0;
                foreach (var order in ok.Orders) { order.Id = 0; order.UserId = 0; }
                db.Users.Add(ok);
            }
            await Save.Invoke(db, async);
            await Commit(retry, async);
        }

        if (flag) AssertCommittedAndDispatchedOnce(host, "fixed@test");
        else AssertCommittedAndDispatchedOnce(host, "ok@test", "fixed@test");
        Assert.Empty(db.Outbox.GetEvents());
    }
}
