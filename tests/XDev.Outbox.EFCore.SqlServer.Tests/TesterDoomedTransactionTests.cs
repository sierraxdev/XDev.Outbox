using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Adversarial (tester) checks of the D5 fix with real SQL Server errors (own throwaway database). CHECK constraints
/// (triggers are not usable: EF's OUTPUT clause forbids them) reject a user whose e-mail contains "failinsert", and
/// a payload UPDATE of a user whose e-mail contains "failupdate" (the INSERT payload has "id":0 and passes; the
/// UPDATE writes the identity value and fails). Error 547. With SET XACT_ABORT ON the error dooms and rolls back the
/// whole transaction, so the library's ROLLBACK TRANSACTION __XDevOutboxSavePoint fails; with XACT_ABORT OFF only the
/// statement fails and the rollback to the savepoint works.
/// </summary>
public class TesterDoomedTransactionTests : IClassFixture<SmokeDatabaseFixture>
{
    private const int NotUndoneEventId = 41002;
    private const int CheckViolation = 547;

    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterDoomedTransactionTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, SmokeDbContext> CreateHost(LogCapture log)
    {
        var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions(), services => services.AddSingleton<ILoggerProvider>(log));
        host.Database.Execute("""
            IF OBJECT_ID(N'[dbo].[CK_Outbox_FailUpdate]') IS NULL
                ALTER TABLE [dbo].[OutboxMessages] ADD CONSTRAINT [CK_Outbox_FailUpdate]
                    CHECK ([Payload] IS NULL OR [Payload] NOT LIKE N'%failupdate%' OR [Payload] LIKE N'%"id":0,%');
            IF OBJECT_ID(N'[dbo].[CK_Users_FailInsert]') IS NULL
                ALTER TABLE [dbo].[Users] ADD CONSTRAINT [CK_Users_FailInsert] CHECK ([Email] NOT LIKE N'%failinsert%');
            """);
        return host;
    }

    private static async Task SetXactAbort(DbContext db, bool on, bool async)
    {
        var sql = on ? "SET XACT_ABORT ON" : "SET XACT_ABORT OFF";
        if (async) await db.Database.ExecuteSqlRawAsync(sql); else db.Database.ExecuteSqlRaw(sql);
    }

    private static async Task<(int Users, int Outbox)> CountsInTransaction(SmokeDbContext db)
        => (await db.Users.AsNoTracking().CountAsync(), await db.Set<OutboxMessage<int>>().AsNoTracking().CountAsync());

    private static void AssertCommittedAndDispatchedOnce(SmokeHost<int, SmokeDbContext> host, params string[] emails)
    {
        var users = host.Database.Users();
        var rows = host.OutboxRows();
        Assert.Equal(emails.OrderBy(x => x), users.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Count, rows.Count);
        GraphAssert.RowsMatch(rows, users, host.Database.Orders());
        Assert.Equal(rows.Select(r => r.Id).OrderBy(x => x), host.Queue.Enqueued.Select(e => e.Id.ToString()).OrderBy(x => x));
    }

    public enum DoomPoint
    {
        /// <summary>The payload UPDATE (after EF's save) violates the CHECK constraint.</summary>
        PayloadUpdate,

        /// <summary>The Users INSERT (inside EF's save) violates the CHECK constraint; EF's own savepoint rollback fails too.</summary>
        UsersInsert,
    }

    /// <summary>
    /// XACT_ABORT ON: the server rolls back the whole transaction. The rollback to the library's savepoint fails, the
    /// original error (547) surfaces, warning 41002 carries the rollback error, the tracker is restored, the caller's
    /// rollback does not throw, and a new transaction persists everything once.
    /// </summary>
    [SqlServerTheory]
    [InlineData(DoomPoint.PayloadUpdate, false)]
    [InlineData(DoomPoint.PayloadUpdate, true)]
    [InlineData(DoomPoint.UsersInsert, false)]
    [InlineData(DoomPoint.UsersInsert, true)]
    public async Task DoomedTransaction_RollbackToSavepointFails_OriginalErrorWarningAndCallerRollbackThenRetryPersistsOnce(DoomPoint point, bool async)
    {
        var log = new LogCapture();
        using var host = CreateHost(log);
        await using var scope = host.CreateScope();
        var db = scope.Db;

        var user = Graph.NewUser(point == DoomPoint.PayloadUpdate ? "failupdate@test" : "failinsert@test");
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();
        try
        {
            await SetXactAbort(db, on: true, async);
            var error = async ? await Record.ExceptionAsync(() => db.SaveChangesAsync()) : Record.Exception(() => db.SaveChanges());
            _output.WriteLine($"Save error: {error?.GetType().Name}: {error?.Message}");

            var sql = error as SqlException ?? error?.InnerException as SqlException;
            Assert.NotNull(sql);
            Assert.Equal(CheckViolation, sql!.Number);
            // The payload UPDATE's error is not wrapped (raw SQL); EF wraps its own save's error in DbUpdateException.
            if (point == DoomPoint.PayloadUpdate) Assert.IsType<SqlException>(error);
            else Assert.IsType<DbUpdateException>(error);

            var warning = Assert.Single(log.WithEventId(NotUndoneEventId));
            Assert.Equal(LogLevel.Warning, warning.Level);
            Assert.NotNull(warning.Exception);
            _output.WriteLine($"Warning exception: {warning.Exception!.GetType().Name}: {warning.Exception.Message}");

            Assert.Equal(EntityState.Added, db.Entry(user).State);
            Assert.Equal(0, user.Id);
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            Assert.Empty(host.Queue.Enqueued);

            var rollbackError = async ? await Record.ExceptionAsync(() => tx.RollbackAsync()) : Record.Exception(() => tx.Rollback());
            _output.WriteLine($"Caller rollback: {rollbackError?.GetType().Name}: {rollbackError?.Message}");
            Assert.Null(rollbackError);
        }
        finally
        {
            if (async) await tx.DisposeAsync(); else tx.Dispose();
            await SetXactAbort(db, on: false, async);
        }

        Assert.Empty(host.Database.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);

        // The failed save left no pending message, so its event was never saved: the caller's rollback keeps it, and
        // it follows the user that stays Added in the tracker (before fix-rollback-event-loss, it was dropped and the
        // application had to add it again).
        Assert.Single(db.Outbox.GetEvents());
        user.Email = "fixed@test";
        await using (var retry = await db.Database.BeginTransactionAsync())
        {
            await db.SaveChangesAsync();
            await retry.CommitAsync();
        }

        AssertCommittedAndDispatchedOnce(host, "fixed@test");
    }

    /// <summary>
    /// XACT_ABORT OFF: the CHECK violation fails only the payload UPDATE; the library rolls back to its savepoint, the
    /// original SqlException surfaces, no warning, and the retry in the same transaction persists once.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StatementErrorInPayloadUpdate_RolledBackToSavepoint_RetryInSameTransactionPersistsOnce(bool async)
    {
        var log = new LogCapture();
        using var host = CreateHost(log);
        await using var scope = host.CreateScope();
        var db = scope.Db;

        var user = Graph.NewUser("failupdate@test");
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        await using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            var error = async ? await Record.ExceptionAsync(() => db.SaveChangesAsync()) : Record.Exception(() => db.SaveChanges());
            Assert.Equal(CheckViolation, Assert.IsType<SqlException>(error).Number);
            Assert.Contains("RolledBackToSavepoint", host.Faults.TransactionEvents);
            Assert.Empty(log.WithEventId(NotUndoneEventId));
            Assert.Equal((0, 0), await CountsInTransaction(db));
            Assert.Equal(EntityState.Added, db.Entry(user).State);

            user.Email = "fixed@test";
            if (async) await db.SaveChangesAsync(); else db.SaveChanges();
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        AssertCommittedAndDispatchedOnce(host, "fixed@test");
    }

    /// <summary>Retries the whole unit of work on error 547 (treated as transient for this test only).</summary>
    private sealed class CheckViolationRetryStrategy : SqlServerRetryingExecutionStrategy
    {
        public CheckViolationRetryStrategy(DbContext context)
            : base(context, 3, TimeSpan.Zero, new[] { CheckViolation })
        {
        }

        protected override TimeSpan? GetNextDelay(Exception lastException)
            => base.GetNextDelay(lastException) is null ? null : TimeSpan.Zero;
    }

    /// <summary>
    /// The application wraps "begin transaction, save, commit" in a retrying execution strategy (XACT_ABORT ON). The
    /// doomed first attempt rethrows the original SqlException, which the strategy recognizes; the second attempt
    /// persists and dispatches everything once.
    /// </summary>
    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DoomedTransactionInsideRetryingStrategy_OriginalErrorIsRetriedAndSecondAttemptPersistsOnce(bool async)
    {
        var log = new LogCapture();
        using var host = CreateHost(log);
        await using var scope = host.CreateScope();
        var db = scope.Db;

        var user = Graph.NewUser("failupdate@test");
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        var attempts = 0;
        var eventsBufferedAtRetry = -1;
        void BeforeAttempt()
        {
            attempts++;
            if (attempts == 1) return;
            user.Email = "fixed@test";
            eventsBufferedAtRetry = db.Outbox.GetEvents().Count;
        }

        var strategy = new CheckViolationRetryStrategy(db);
        try
        {
            if (async)
            {
                await strategy.ExecuteAsync(async () =>
                {
                    BeforeAttempt();
                    await using var tx = await db.Database.BeginTransactionAsync();
                    await SetXactAbort(db, on: true, async: true);
                    await db.SaveChangesAsync();
                    await tx.CommitAsync();
                });
            }
            else
            {
                strategy.Execute(() =>
                {
                    BeforeAttempt();
                    using var tx = db.Database.BeginTransaction();
                    db.Database.ExecuteSqlRaw("SET XACT_ABORT ON");
                    db.SaveChanges();
                    tx.Commit();
                });
            }
        }
        finally
        {
            await SetXactAbort(db, on: false, async);
        }

        Assert.Equal(2, attempts);
        // The failed attempt's transaction is disposed without an explicit rollback: the next save detects it as
        // stale, which keeps never-saved events, so the retry saves the event with its user. (An explicit Rollback
        // keeps it too: see TesterAdversarialSavepointTests.FailedSave_CallerRollsBackOrDisposes_...)
        Assert.Equal(1, eventsBufferedAtRetry);
        Assert.Single(log.WithEventId(NotUndoneEventId));
        AssertCommittedAndDispatchedOnce(host, "fixed@test");
    }
}
