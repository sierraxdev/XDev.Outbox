using System.Reflection;
using System.Reflection.Emit;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

// Task fix-claim-column-names-and-long-type-names, adversarial tester (2026-11-20).
// Bug 2: column names that are swapped between outbox properties (EF allows it), that equal the claim/renewal SQL's
// own aliases (cte, v, t, Expected) or SQL keywords. With a swap every hard-coded or mixed-up column name still parses
// and runs, so only the results reveal a wrong column (int vs datetime2 vs bit vs uniqueidentifier).
// Bug 5: the type-name length check at the exact boundary with supplementary characters, a consumer column widened with
// HasColumnType only, a retrying execution strategy, and long names together with renamed columns.

public static class SwappedColumnSets
{
    /// <summary>Id and LockExpiresAt swapped, Attemps and IsError swapped; the rest named like the SQL's aliases.</summary>
    public static OutboxColumnNames IdLockSwap { get; } = new(
        Id: "LockExpiresAt",
        Type: "t",
        Payload: "Expected",
        Created: "v",
        LockExpiresAt: "Id",
        Attemps: "IsError",
        IsError: "Attemps",
        ErrorMessage: "cte");

    /// <summary>Id and Attemps swapped, LockExpiresAt and Created swapped; the rest are SQL keywords.</summary>
    public static OutboxColumnNames IdAttempsSwap { get; } = new(
        Id: "Attemps",
        Type: "Order",
        Payload: "Select",
        Created: "LockExpiresAt",
        LockExpiresAt: "Created",
        Attemps: "Id",
        IsError: "Key",
        ErrorMessage: "User");
}

public sealed class IdLockSwapColumnsDbContext : SmokeContextBase<int>
{
    public IdLockSwapColumnsDbContext(DbContextOptions<IdLockSwapColumnsDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder) => SwappedColumnSets.IdLockSwap.Apply<int>(builder);
}

public sealed class IdAttempsSwapColumnsDbContext : SmokeContextBase<int>
{
    public IdAttempsSwapColumnsDbContext(DbContextOptions<IdAttempsSwapColumnsDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder) => SwappedColumnSets.IdAttempsSwap.Apply<int>(builder);
}

public sealed class GuidIdAttempsSwapColumnsDbContext : SmokeContextBase<Guid>
{
    public GuidIdAttempsSwapColumnsDbContext(DbContextOptions<GuidIdAttempsSwapColumnsDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder) => SwappedColumnSets.IdAttempsSwap.Apply<Guid>(builder);
}

/// <summary>The full renamed-column scenario suite (save, 12 claim combinations, concurrency, renewal, hosted service) with swapped names.</summary>
public class TesterIdLockSwapColumnsScenarioTests : RenamedColumnScenarioTests<int, IdLockSwapColumnsDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public TesterIdLockSwapColumnsScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => SwappedColumnSets.IdLockSwap;
}

public class TesterIdAttempsSwapColumnsScenarioTests : RenamedColumnScenarioTests<int, IdAttempsSwapColumnsDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public TesterIdAttempsSwapColumnsScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => SwappedColumnSets.IdAttempsSwap;
}

public class TesterGuidIdAttempsSwapColumnsScenarioTests : RenamedColumnScenarioTests<Guid, GuidIdAttempsSwapColumnsDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public TesterGuidIdAttempsSwapColumnsScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => SwappedColumnSets.IdAttempsSwap;
}

/// <summary>The consumer widened the Type column with HasColumnType only; the model's max length stays 200.</summary>
public sealed class WideTypeColumnTypeOnlyDbContext : SmokeContextBase<int>
{
    public WideTypeColumnTypeOnlyDbContext(DbContextOptions<WideTypeColumnTypeOnlyDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().Property(x => x.Type).HasColumnType("nvarchar(500)");
}

/// <summary>Builds event types with an exact stored-name length (dynamic assembly; never resolvable, only saved).</summary>
internal static class ExactLengthEventTypes
{
    private static readonly AssemblyBuilder Assembly = AssemblyBuilder.DefineDynamicAssembly(
        new AssemblyName { Name = "XDevTesterExactLen" }, AssemblyBuilderAccess.Run);

    private static readonly ModuleBuilder Module = Assembly.DefineDynamicModule("m");
    private static readonly object Gate = new();
    private static readonly Dictionary<string, Type> Types = new();

    /// <summary>
    /// A type whose stored name "<c>Ns.Ev😀...x, XDevTesterExactLen</c>" is exactly <paramref name="length"/> UTF-16 units,
    /// with <paramref name="supplementary"/> supplementary-plane characters (2 units each) in the type name.
    /// </summary>
    public static Type WithStoredNameLength(int length, int supplementary)
    {
        const string suffix = ", XDevTesterExactLen";
        var prefix = "Tester.Ev" + string.Concat(Enumerable.Repeat("\U0001F600", supplementary)) + "_" + length + "_";
        var name = prefix.PadRight(length - suffix.Length, 'x');
        lock (Gate)
        {
            if (!Types.TryGetValue(name, out var type))
            {
                type = Module.DefineType(name, TypeAttributes.Public | TypeAttributes.Class).CreateType()!;
                Types[name] = type;
            }
            Assert.Equal(length, StoredTypeName.Of(type).Length);
            return type;
        }
    }
}

public class TesterTypeNameLengthSqlServerTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterTypeNameLengthSqlServerTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [SqlServerTheory]
    [InlineData(199, false)]
    [InlineData(200, false)]
    [InlineData(199, true)]
    [InlineData(200, true)]
    public async Task Save_StoredNameUpToTheMaxLengthWithSupplementaryCharacters_IsStoredExactly(int length, bool async)
    {
        var type = ExactLengthEventTypes.WithStoredNameLength(length, supplementary: 5);
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        await using var scope = host.CreateScope();
        scope.Db.Outbox.AddEvent(Activator.CreateInstance(type)!);

        await Save.Invoke(scope.Db, async);

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(StoredTypeName.Of(type), row.Type);
        Assert.Equal(length, row.Type!.Length);
        // SQL Server counts nvarchar length in byte pairs, like .NET's UTF-16 Length: a supplementary character is 2.
        Assert.Equal(length, Convert.ToInt32(host.Database.Scalar($"SELECT LEN({host.OutboxColumns.Type}) FROM {host.OutboxTable}")));
        Assert.Single(host.Queue.Enqueued);
    }

    [SqlServerTheory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Save_StoredNameOneOverTheMaxLengthWithSupplementaryCharacters_ThrowsBeforeAnyCommand(bool callerTransaction, bool async)
    {
        var type = ExactLengthEventTypes.WithStoredNameLength(201, supplementary: 5);
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("boundary@test");
        scope.Db.Users.Add(user);
        var tooLong = Activator.CreateInstance(type)!;
        scope.Db.Outbox.AddEvent(tooLong);
        await using var tx = callerTransaction ? await scope.Db.Database.BeginTransactionAsync() : null;
        host.Faults.ClearLog();

        var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));

        var invalid = Assert.IsType<InvalidOperationException>(error);
        Assert.Contains("201 characters", invalid.Message);
        Assert.Contains("allows at most 200", invalid.Message);
        Assert.Empty(host.Faults.Commands);
        Assert.Empty(host.Faults.TransactionEvents);
        Assert.Equal(new[] { tooLong }, scope.Db.Outbox.GetEvents());
        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
        Assert.Same(tx, scope.Db.Database.CurrentTransaction);
        if (tx != null) await tx.RollbackAsync();
        Assert.Empty(host.Database.Users());
        Assert.Empty(host.OutboxRows());
    }

    [SqlServerFact]
    public void Column_Nvarchar200_AcceptsTheSame200UnitStringAndRejects201_AsTheCheckCounts()
    {
        // The check's unit (UTF-16 code units) is the column's unit: the raw 200-unit name fits, 201 is truncation 2628.
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        var c = host.OutboxColumns;
        var insert = $"INSERT INTO {host.OutboxTable} ({c.Type}, {c.Payload}, {c.Created}) VALUES (@type, N'{{}}', SYSUTCDATETIME())";
        host.Database.Execute(insert, ("@type", StoredTypeName.Of(ExactLengthEventTypes.WithStoredNameLength(200, supplementary: 5))));
        var error = Assert.Throws<SqlException>(() => host.Database.Execute(insert, ("@type", StoredTypeName.Of(ExactLengthEventTypes.WithStoredNameLength(201, supplementary: 5)))));
        _output.WriteLine($"{error.Number}: {error.Message}");
        Assert.Contains(error.Number, new[] { 2628, 8152 });
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_TooLongNameWithARetryingExecutionStrategy_ThrowsOnceWithoutRetryingOrSendingCommands(bool async)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture, new SmokeHostOptions { RetryingStrategy = true });
        await using var scope = host.CreateScope();
        scope.Db.Users.Add(Graph.NewUser("retry@test"));
        scope.Db.Outbox.AddEvent(Activator.CreateInstance(ExactLengthEventTypes.WithStoredNameLength(201, supplementary: 0))!);
        host.Faults.ClearLog();

        var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));

        Assert.IsType<InvalidOperationException>(error); // not wrapped in a RetryLimitExceededException
        Assert.Empty(host.Faults.Commands);
        Assert.Empty(host.Faults.TransactionEvents);
        Assert.Empty(host.Database.Users());
        Assert.Empty(host.OutboxRows());
    }

    [SqlServerFact]
    public async Task Save_ManyEventsOfOneTypeAndOneTooLongEventLast_ThrowsAndWritesNothing()
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        await using var scope = host.CreateScope();
        var events = Enumerable.Range(0, 500).Select(i => (object)new TextEvent { Text = "t" + i }).ToList();
        events.Add(Activator.CreateInstance(ExactLengthEventTypes.WithStoredNameLength(201, supplementary: 1))!);
        scope.Db.Outbox.AddEvent(events.ToArray());
        host.Faults.ClearLog();

        var error = await Record.ExceptionAsync(() => scope.Db.SaveChangesAsync());

        Assert.IsType<InvalidOperationException>(error);
        Assert.Empty(host.Faults.Commands);
        Assert.Equal(501, scope.Db.Outbox.GetEvents().Count);
        Assert.Empty(host.OutboxRows());

        // Without the long event the 500 duplicates of one type are saved.
        scope.Db.Outbox.ClearEvents();
        scope.Db.Outbox.AddEvent(events.Take(500).ToArray());
        await scope.Db.SaveChangesAsync();
        Assert.Equal(500, host.OutboxRows().Count);
        Assert.All(host.OutboxRows(), r => Assert.Equal(StoredTypeName.Of<TextEvent>(), r.Type));
    }
}

/// <summary>Own fixture: its table has a different Type column than SmokeDbContext's.</summary>
public class TesterWideTypeColumnTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TesterWideTypeColumnTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>
    /// A consumer who widened the column with HasColumnType("nvarchar(500)") but not HasMaxLength (the model's max length
    /// stays 200): the length check follows the explicit column type, so a name of 201..500 characters that the column
    /// holds is saved (before the developer correction it was rejected; before the fix the full name was saved).
    /// </summary>
    [SqlServerFact]
    public async Task Save_ColumnWidenedWithHasColumnTypeOnly_NameThatFitsTheColumnButNotTheModelMaxLength_IsSaved()
    {
        var type = typeof(LongNamedEvent<List<User>>);
        using var host = SmokeHost<int, WideTypeColumnTypeOnlyDbContext>.Create(_fixture);
        Assert.Equal(500, Convert.ToInt32(host.Database.Scalar(
            $"SELECT COL_LENGTH(@table, N'Type') / 2", ("@table", host.OutboxTable))));
        await using var scope = host.CreateScope();
        Assert.Equal(200, scope.Db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindProperty("Type")!.GetMaxLength());
        scope.Db.Outbox.AddEvent(new LongNamedEvent<List<User>>());

        await scope.Db.SaveChangesAsync();

        var stored = Assert.Single(host.OutboxRows()).Type!;
        _output.WriteLine($"full {type.AssemblyQualifiedName!.Length}, stored {stored.Length}: {stored}");
        Assert.InRange(stored.Length, 201, 500);
        Assert.DoesNotContain("Version=", stored);
        Assert.Same(type, Type.GetType(stored, throwOnError: true));
    }
}

/// <summary>The consumer narrowed/widened the Type column with HasColumnType("nvarchar(250)") only.</summary>
public sealed class TypeColumn250DbContext : SmokeContextBase<int>
{
    public TypeColumn250DbContext(DbContextOptions<TypeColumn250DbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().Property(x => x.Type).HasColumnType("nvarchar(250)");
}

/// <summary>Own fixture: its table has a different Type column than SmokeDbContext's.</summary>
public class TypeColumn250Tests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TypeColumn250Tests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>The explicit column type's length is the limit: 250 fits, a 257-character name is rejected before any command.</summary>
    [SqlServerFact]
    public async Task Save_ColumnTypeNVarChar250_NameOf250IsSavedAndNameOf257IsRejected()
    {
        using var host = SmokeHost<int, TypeColumn250DbContext>.Create(_fixture);
        Assert.Equal(250, Convert.ToInt32(host.Database.Scalar(
            $"SELECT COL_LENGTH(@table, N'Type') / 2", ("@table", host.OutboxTable))));

        var fits = ExactLengthEventTypes.WithStoredNameLength(250, 0);
        var tooLong = ExactLengthEventTypes.WithStoredNameLength(257, 0);
        await using (var scope = host.CreateScope())
        {
            scope.Db.Outbox.AddEvent(Activator.CreateInstance(fits)!);
            await scope.Db.SaveChangesAsync();
        }
        Assert.Equal(250, Assert.Single(host.OutboxRows()).Type!.Length);

        await using (var scope = host.CreateScope())
        {
            var @event = Activator.CreateInstance(tooLong)!;
            scope.Db.Outbox.AddEvent(@event);
            host.Faults.ClearLog();

            var error = await Record.ExceptionAsync(() => scope.Db.SaveChangesAsync());

            var invalid = Assert.IsType<InvalidOperationException>(error);
            _output.WriteLine(invalid.Message);
            Assert.Contains("257 characters", invalid.Message);
            Assert.Contains("allows at most 250 (column type 'nvarchar(250)')", invalid.Message);
            Assert.Empty(host.Faults.Commands);
            Assert.Equal(new[] { @event }, scope.Db.Outbox.GetEvents());
        }
        Assert.Single(host.OutboxRows());
    }
}

/// <summary>Bug 2 + Bug 5 together: swapped/alias column names (Type stored in a column named "t") and long generic names.</summary>
public class TesterSwappedColumnsLongTypeNameTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public TesterSwappedColumnsLongTypeNameTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    private static async Task WaitUntil(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Timed out waiting for: " + what);
            await Task.Delay(50);
        }
    }

    [SqlServerFact]
    public async Task HostedService_LongGenericEventSavedAndSeededWithVersionLessName_DispatchedByImmediateAndPollerThroughSwappedColumns()
    {
        using var host = SmokeHost<int, IdLockSwapColumnsDbContext>.Create(_fixture, new SmokeHostOptions { RealQueue = true });
        var c = host.OutboxColumns;
        Assert.Equal("[t]", c.Type);
        Assert.Equal("[LockExpiresAt]", c.Id);
        // A pending row from another instance, with the version-less name, unlocked: the poller claims it at start.
        host.Database.Execute(
            $"INSERT INTO {host.OutboxTable} ({c.Type}, {c.Payload}, {c.Created}, {c.LockExpiresAt}) VALUES (@type, @payload, SYSUTCDATETIME(), NULL)",
            ("@type", OwnedSaveTests.LongNamedUserEventStoredName),
            ("@payload", "{\"user\":{\"id\":42,\"email\":\"poller@test\",\"orders\":[]}}"));
        var probe = host.Services.GetRequiredService<SubscriberProbe>();
        var services = host.Services.GetServices<IHostedService>().ToList();
        foreach (var service in services) await service.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntil(() => probe.Received.Count >= 1 && host.OutboxRows().Count == 0, "poller dispatch");

            await using (var scope = host.CreateScope())
            {
                var user = Graph.NewUser("immediate@test");
                scope.Db.Users.Add(user);
                scope.Db.Outbox.AddEvent(new LongNamedEvent<User> { User = user });
                await scope.Db.SaveChangesAsync();
            }
            await WaitUntil(() => probe.Received.Count >= 2 && host.OutboxRows().Count == 0, "immediate dispatch");
            await Task.Delay(300);

            Assert.Equal(new[] { "long:poller@test", "long:immediate@test" }, probe.Received.Select(r => r.Text));

            // A name the column cannot hold is still rejected before any command with the renamed columns.
            await using (var scope = host.CreateScope())
            {
                scope.Db.Outbox.AddEvent(new LongNamedEvent<Dictionary<string, List<KeyValuePair<int, Guid>>>>());
                host.Faults.ClearLog();
                Assert.IsType<InvalidOperationException>(await Record.ExceptionAsync(() => scope.Db.SaveChangesAsync()));
                Assert.DoesNotContain(host.Faults.Commands, cmd => cmd.Sql.Contains("INSERT", StringComparison.OrdinalIgnoreCase));
            }
            Assert.Empty(host.OutboxRows());
            Assert.Equal(2, probe.Received.Count);
        }
        finally
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in services) await service.StopAsync(cts.Token);
        }
    }
}
