using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>Entity with a SQL Server rowversion concurrency token.</summary>
public class VersionedDoc
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public byte[] RowVersion { get; set; } = Array.Empty<byte>();
}

/// <summary>Entity whose Guid key is assigned by the application (never generated).</summary>
public class ClientKeyDoc
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
}

public sealed class PartialSaveDbContext : SmokeContextBase<int>
{
    public PartialSaveDbContext(DbContextOptions<PartialSaveDbContext> options) : base(options)
    {
    }

    public DbSet<VersionedDoc> VersionedDocs => Set<VersionedDoc>();
    public DbSet<ClientKeyDoc> ClientKeyDocs => Set<ClientKeyDoc>();

    protected override void AfterOutboxConfigured(ModelBuilder builder)
    {
        builder.Entity<VersionedDoc>(b =>
        {
            b.ToTable("VersionedDocs");
            b.Property(x => x.RowVersion).IsRowVersion();
        });
        builder.Entity<ClientKeyDoc>(b =>
        {
            b.ToTable("ClientKeyDocs");
            b.Property(x => x.Id).ValueGeneratedNever();
        });
    }
}

/// <summary>
/// D5 on SQL Server: a save with events inside a caller-owned transaction that fails after EF's save (payload UPDATE)
/// is undone by the library's savepoint (SAVE TRANSACTION __XDevOutboxSavePoint), so retrying in the same transaction
/// works for rows whose retry would otherwise fail (rowversion, client-assigned keys). With MARS, SQL Server
/// savepoints are not used (by EF nor by the library): the failure logs warning 41002 and the caller must roll back.
/// </summary>
public class CallerTransactionPartialSaveTests : IClassFixture<SmokeDatabaseFixture>
{
    private const int NotUndoneEventId = 41002;

    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public CallerTransactionPartialSaveTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, PartialSaveDbContext> CreateHost(LogCapture? log = null, bool mars = false)
        => SmokeHost<int, PartialSaveDbContext>.Create(
            _fixture,
            new SmokeHostOptions { MultipleActiveResultSets = mars },
            services =>
            {
                if (log != null) services.AddSingleton<ILoggerProvider>(log);
            });

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

    private static void AssertUsersPersistedAndDispatchedOnce(SmokeHost<int, PartialSaveDbContext> host, int users)
    {
        var dbUsers = host.Database.Users();
        var orders = host.Database.Orders();
        var rows = host.OutboxRows();
        Assert.Equal(users, dbUsers.Count);
        Assert.Equal(users, rows.Count);
        GraphAssert.RowsMatch(rows, dbUsers, orders);
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id.ToString()));
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
    }

    public enum OtherEntity
    {
        /// <summary>Modified row with a rowversion: without the rollback the retried UPDATE matches no row (concurrency failure).</summary>
        RowVersion,

        /// <summary>Added row with an application-assigned Guid key: without the rollback the retried INSERT violates the key.</summary>
        ClientKey,
    }

    public static TheoryData<OtherEntity, FaultTiming, bool> OtherEntityTimingAsync()
    {
        var data = new TheoryData<OtherEntity, FaultTiming, bool>();
        foreach (var entity in Enum.GetValues<OtherEntity>())
        foreach (var timing in Enum.GetValues<FaultTiming>())
        foreach (var async in new[] { false, true })
            data.Add(entity, timing, async);
        return data;
    }

    [SqlServerTheory]
    [MemberData(nameof(OtherEntityTimingAsync))]
    public async Task PayloadUpdateFailsWithOtherEntityInTheSave_RetryInTheSameTransactionSucceeds(OtherEntity other, FaultTiming timing, bool async)
    {
        using var host = CreateHost();
        host.Database.Execute("INSERT INTO [VersionedDocs] ([Name]) VALUES (N'before')");
        await using var scope = host.CreateScope();
        var db = scope.Db;

        var doc = await db.VersionedDocs.SingleAsync();
        var clientKey = Guid.NewGuid();
        if (other == OtherEntity.RowVersion) doc.Name = "after";
        else db.ClientKeyDocs.Add(new ClientKeyDoc { Id = clientKey, Title = "t" });
        var user = Graph.NewUser("partial@test", "p1");
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, timing);

        await using (var tx = await Begin(db, async))
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            Assert.IsType<InjectedTransientException>(error);
            Assert.Equal(1, host.Faults.CommandFailuresFired);
            Assert.Contains("RolledBackToSavepoint", host.Faults.TransactionEvents);
            Assert.Equal(other == OtherEntity.RowVersion ? EntityState.Modified : EntityState.Unchanged, db.Entry(doc).State);

            var retryError = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            Assert.Null(retryError);
            await Commit(tx, async);
        }

        AssertUsersPersistedAndDispatchedOnce(host, users: 1);
        if (other == OtherEntity.RowVersion) Assert.Equal("after", host.Database.Scalar("SELECT [Name] FROM [VersionedDocs]"));
        else Assert.Equal(clientKey, host.Database.Scalar("SELECT [Id] FROM [ClientKeyDocs]"));
        _output.WriteLine("Transaction events: " + string.Join(", ", host.Faults.TransactionEvents));
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Mars_SaveInCallerTransactionThenCommit_WorksWithoutSavepoints(bool async)
    {
        var log = new LogCapture();
        using var host = CreateHost(log, mars: true);
        await using var scope = host.CreateScope();

        await using (var tx = await Begin(scope.Db, async))
        {
            var first = Graph.NewUser("mars-1@test", "m1");
            scope.Db.Users.Add(first);
            scope.Db.Outbox.AddEvent(new UserCreated { User = first }, new OrderPlaced { Order = first.Orders[0] });
            await Save.Invoke(scope.Db, async);

            var second = Graph.NewUser("mars-2@test");
            scope.Db.Users.Add(second);
            scope.Db.Outbox.AddEvent(new UserCreated { User = second });
            await Save.Invoke(scope.Db, async);

            Assert.Empty(host.Queue.Enqueued);
            await Commit(tx, async);
        }

        var users = host.Database.Users();
        var rows = host.OutboxRows();
        Assert.Equal(2, users.Count);
        Assert.Equal(3, rows.Count);
        GraphAssert.RowsMatch(rows, users, host.Database.Orders());
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id.ToString()));
        Assert.DoesNotContain("CreatedSavepoint", host.Faults.TransactionEvents);
        Assert.DoesNotContain(host.Faults.Commands, c => c.Sql.Contains("SAVE TRANSACTION", StringComparison.OrdinalIgnoreCase));
        Assert.Empty(log.WithEventId(NotUndoneEventId));
        _output.WriteLine("Transaction events: " + string.Join(", ", host.Faults.TransactionEvents));
    }

    /// <summary>
    /// Documented fallback (Q1): with MARS there is no savepoint, so a failure after EF's save leaves the rows in the
    /// caller's transaction and logs warning 41002; rolling back and saving again persists everything once.
    /// </summary>
    [SqlServerTheory]
    [InlineData(FaultTiming.BeforeExecution, false)]
    [InlineData(FaultTiming.BeforeExecution, true)]
    [InlineData(FaultTiming.AfterExecution, false)]
    [InlineData(FaultTiming.AfterExecution, true)]
    public async Task Mars_PayloadUpdateFails_WarningLoggedAndRollbackThenRetryPersistsOnce(FaultTiming timing, bool async)
    {
        var log = new LogCapture();
        using var host = CreateHost(log, mars: true);
        await using var scope = host.CreateScope();
        var db = scope.Db;

        var user = Graph.NewUser("mars-fail@test", "f1");
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsOutboxPayloadUpdate, timing);

        await using (var tx = await Begin(db, async))
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            Assert.IsType<InjectedTransientException>(error);

            var warning = Assert.Single(log.WithEventId(NotUndoneEventId));
            Assert.Equal(LogLevel.Warning, warning.Level);
            Assert.Equal("XDev.Outbox.EFCore.CallerTransactionSaveNotUndone", warning.EventId.Name);
            Assert.Equal("Microsoft.EntityFrameworkCore.Database.Transaction", warning.Category);
            Assert.Equal(1, await db.Users.AsNoTracking().CountAsync()); // nothing undone
            Assert.Equal(EntityState.Added, db.Entry(user).State);

            await Rollback(tx, async);
        }

        Assert.Empty(host.Database.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Single(db.Outbox.GetEvents()); // never saved: kept by the rollback

        await using (var tx = await Begin(db, async))
        {
            await Save.Invoke(db, async);
            await Commit(tx, async);
        }

        AssertUsersPersistedAndDispatchedOnce(host, users: 1);
        Assert.Single(log.WithEventId(NotUndoneEventId));
        Assert.DoesNotContain("CreatedSavepoint", host.Faults.TransactionEvents);
    }
}

public sealed record LogEntry(string Category, LogLevel Level, EventId EventId, string Message, Exception? Exception);

/// <summary>Captures every log entry (registered as an <see cref="ILoggerProvider"/> in the application services).</summary>
public sealed class LogCapture : ILoggerProvider
{
    private readonly object _gate = new();
    private readonly List<LogEntry> _entries = new();

    public IReadOnlyList<LogEntry> Entries
    {
        get { lock (_gate) return _entries.ToArray(); }
    }

    public IReadOnlyList<LogEntry> WithEventId(int id) => Entries.Where(e => e.EventId.Id == id).ToList();

    public ILogger CreateLogger(string categoryName) => new CaptureLogger(this, categoryName);

    public void Dispose()
    {
    }

    private void Add(LogEntry entry)
    {
        lock (_gate) _entries.Add(entry);
    }

    private sealed class CaptureLogger : ILogger
    {
        private readonly LogCapture _owner;
        private readonly string _category;

        public CaptureLogger(LogCapture owner, string category)
        {
            _owner = owner;
            _category = category;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _owner.Add(new LogEntry(_category, logLevel, eventId, formatter(state, exception), exception));
    }
}
