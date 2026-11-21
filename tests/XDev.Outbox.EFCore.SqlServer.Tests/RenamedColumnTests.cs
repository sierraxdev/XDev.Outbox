using System.Diagnostics;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

// Task fix-claim-column-names-and-long-type-names (Bug 2): the claim and renewal SQL must use the outbox columns the
// model maps (HasColumnName, naming conventions), delimited and brace-escaped, like the table name. Every context here
// renames ALL outbox columns and runs in its own throwaway database (one fixture per test class).

/// <summary>Undelimited column names for the eight outbox properties.</summary>
public sealed record OutboxColumnNames(string Id, string Type, string Payload, string Created, string LockExpiresAt, string Attemps, string IsError, string ErrorMessage)
{
    /// <summary>Quotes, closing brackets, format placeholders, doubled braces, comment starters, ';' and parameter-like tokens.</summary>
    public static OutboxColumnNames Hostile { get; } = new(
        Id: "Id] {0} '--",
        Type: "Type {{ }} \"q\"",
        Payload: "Payload; DROP TABLE x --",
        Created: "Created /* @take */",
        LockExpiresAt: "Lock]Expires{1}At ; @lockExpiration",
        Attemps: "Attemps @maxAttemps ]]",
        IsError: "Is Error 'x'",
        ErrorMessage: "Error]Message {{0}}");

    /// <summary>Accents, CJK, a supplementary-plane character, and two 128-character names containing ']' and braces.</summary>
    public static OutboxColumnNames Unicode { get; } = new(
        Id: "Id{]}ü".PadRight(128, 'i'),
        Type: "Tipo ñ 漢字",
        Payload: "Carga 😀",
        Created: "Creado_日本",
        LockExpiresAt: "Bloqueo{]}😀".PadRight(128, 'x'),
        Attemps: "Intentos 😀 ü",
        IsError: "EsError_漢",
        ErrorMessage: "Mensaje de error ñ");

    public static OutboxColumnNames SnakeCase { get; } = new("id", "type", "payload", "created", "lock_expires_at", "attemps", "is_error", "error_message");

    public OutboxColumns Delimited()
    {
        static string D(string name) => "[" + name.Replace("]", "]]") + "]";
        return new OutboxColumns(D(Id), D(Type), D(Payload), D(Created), D(LockExpiresAt), D(Attemps), D(IsError), D(ErrorMessage));
    }

    public IEnumerable<string> All => new[] { Id, Type, Payload, Created, LockExpiresAt, Attemps, IsError, ErrorMessage };

    public void Apply<TKey>(ModelBuilder builder) where TKey : struct
        => builder.Entity<OutboxMessage<TKey>>(b =>
        {
            b.Property(x => x.Id).HasColumnName(Id);
            b.Property(x => x.Type).HasColumnName(Type);
            b.Property(x => x.Payload).HasColumnName(Payload);
            b.Property(x => x.Created).HasColumnName(Created);
            b.Property(x => x.LockExpiresAt).HasColumnName(LockExpiresAt);
            b.Property(x => x.Attemps).HasColumnName(Attemps);
            b.Property(x => x.IsError).HasColumnName(IsError);
            b.Property(x => x.ErrorMessage).HasColumnName(ErrorMessage);
        });
}

public sealed class HostileColumnsDbContext : SmokeContextBase<int>
{
    public HostileColumnsDbContext(DbContextOptions<HostileColumnsDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder) => OutboxColumnNames.Hostile.Apply<int>(builder);
}

public sealed class UnicodeColumnsDbContext : SmokeContextBase<int>
{
    public UnicodeColumnsDbContext(DbContextOptions<UnicodeColumnsDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder) => OutboxColumnNames.Unicode.Apply<int>(builder);
}

public sealed class SnakeCaseColumnsDbContext : SmokeContextBase<int>
{
    public SnakeCaseColumnsDbContext(DbContextOptions<SnakeCaseColumnsDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder) => OutboxColumnNames.SnakeCase.Apply<int>(builder);
}

/// <summary>Guid outbox key with the hostile column names.</summary>
public sealed class GuidHostileColumnsDbContext : SmokeContextBase<Guid>
{
    public GuidHostileColumnsDbContext(DbContextOptions<GuidHostileColumnsDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder) => OutboxColumnNames.Hostile.Apply<Guid>(builder);
}

/// <summary>
/// snake_case columns from a naming convention (a test-only <see cref="IConventionSetPlugin"/>, registered like
/// EFCore.NamingConventions does) instead of HasColumnName. The convention renames the columns of every entity type,
/// including the ad-hoc type the renewal reads its OUTPUT into, unless a [Column] attribute names the column.
/// </summary>
public sealed class SnakeCasePluginDbContext : SmokeContextBase<int>
{
    public SnakeCasePluginDbContext(DbContextOptions<SnakeCasePluginDbContext> options) : base(options)
    {
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        base.OnConfiguring(optionsBuilder);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(new SnakeCaseNamingOptionsExtension());
    }
}

public sealed class SnakeCaseNamingOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
        => new EntityFrameworkServicesBuilder(services).TryAdd<IConventionSetPlugin, SnakeCaseConventionSetPlugin>();

    public void Validate(IDbContextOptions options)
    {
    }

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        public ExtensionInfo(IDbContextOptionsExtension extension) : base(extension)
        {
        }

        public override bool IsDatabaseProvider => false;
        public override string LogFragment => "using test snake_case column naming ";
        public override int GetServiceProviderHashCode() => 0;
        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ExtensionInfo;
        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) => debugInfo["XDev:SnakeCaseColumns"] = "1";
    }
}

public sealed class SnakeCaseConventionSetPlugin : IConventionSetPlugin
{
    public ConventionSet ModifyConventions(ConventionSet conventionSet)
    {
        conventionSet.ModelFinalizingConventions.Add(new SnakeCaseColumnConvention());
        return conventionSet;
    }
}

/// <summary>Names every column snake_case with convention configuration source (explicit names and [Column] win).</summary>
public sealed class SnakeCaseColumnConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            foreach (var property in entityType.GetDeclaredProperties())
            {
                property.Builder.HasColumnName(ToSnakeCase(property.Name));
            }
        }
    }

    public static string ToSnakeCase(string name)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            if (char.IsUpper(name[i]) && i > 0) builder.Append('_');
            builder.Append(char.ToLowerInvariant(name[i]));
        }
        return builder.ToString();
    }
}

/// <summary>Save, claim, update, remove, renewal and the hosted service against an outbox table with renamed columns.</summary>
public abstract class RenamedColumnScenarioTests<TKey, TContext>
    where TKey : struct
    where TContext : SmokeContextBase<TKey>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    protected RenamedColumnScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>The column names the context must map (and the database must have).</summary>
    protected abstract OutboxColumnNames ExpectedNames { get; }

    private SmokeHost<TKey, TContext> CreateHost(SmokeHostOptions? options = null, Action<IServiceCollection>? configure = null)
    {
        var host = SmokeHost<TKey, TContext>.Create(_fixture, options, configure);
        Assert.Equal(ExpectedNames.Delimited(), host.OutboxColumns);
        foreach (var column in ExpectedNames.All)
        {
            Assert.NotNull(host.Database.Scalar("SELECT COL_LENGTH(@table, @column)", ("@table", host.OutboxTable), ("@column", column)));
        }
        // Unless a swapped mapping gives another property that name on purpose (TesterSwappedColumnNameTests).
        if (!ExpectedNames.All.Contains("LockExpiresAt"))
        {
            Assert.Null(host.Database.Scalar("SELECT COL_LENGTH(@table, N'LockExpiresAt')", ("@table", host.OutboxTable)));
        }
        return host;
    }

    private static IOutboxStore<TKey> Store(SmokeScope<TContext> scope) => scope.Services.GetRequiredService<IOutboxStore<TKey>>();

    private static TKey Seed(SmokeHost<TKey, TContext> host, DateTime? lockExpiresAt, int? attemps, string payload = "{\"text\":\"seed\",\"number\":1}")
        => StoreSeed.Row<TKey>(host.Database, host.OutboxTable, host.OutboxColumns, lockExpiresAt, attemps, payload);

    private static TKey Key(string id) => typeof(TKey) == typeof(Guid) ? (TKey)(object)Guid.Parse(id) : (TKey)(object)int.Parse(id);

    private static bool IsClaim(CommandRecord c) => c.Sql.Contains("READPAST", StringComparison.Ordinal);

    private static bool IsRenewal(CommandRecord c) => c.Sql.Contains("INNER JOIN (VALUES", StringComparison.Ordinal);

    private static DateTime PreciseLock(int minutes) => new DateTime(2099, 3, 4, 5, 6, 7, DateTimeKind.Unspecified).AddMinutes(minutes).AddTicks(1234567);

    /// <summary>Inserts <paramref name="count"/> unlocked ProbeEvent rows in one statement.</summary>
    private static void SeedMany(SmokeHost<TKey, TContext> host, int count)
    {
        var c = host.OutboxColumns;
        var guid = typeof(TKey) == typeof(Guid);
        host.Database.Execute(
            $"INSERT INTO {host.OutboxTable} ({(guid ? c.Id + ", " : "")}{c.Type}, {c.Payload}, {c.Created}, {c.LockExpiresAt}) SELECT TOP ({count}) {(guid ? "NEWID(), " : "")}@type, N'{{}}', SYSUTCDATETIME(), NULL FROM sys.all_objects a CROSS JOIN sys.all_objects b",
            ("@type", typeof(ProbeEvent).AssemblyQualifiedName));
    }

    [SqlServerFact]
    public async Task SaveWithPayloadUpdate_ClaimUpdateAndRemove_UseTheRenamedColumns()
    {
        using var host = CreateHost();
        var c = host.OutboxColumns;
        await using (var scope = host.CreateScope())
        {
            var user = Graph.NewUser("renamed@test", "r1");
            scope.Db.Users.Add(user);
            scope.Db.Outbox.AddEvent(new UserCreated { User = user }, new TextEvent { Text = "renamed {0} ]" });
            await scope.Db.SaveChangesAsync();
        }

        // The save (INSERT + payload UPDATE) and the recording queue's row probe use the renamed columns.
        Assert.Equal(2, host.Queue.Enqueued.Count);
        Assert.All(host.Queue.Enqueued, e => Assert.Equal("committed", e.RowStateAtEnqueue));
        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        await using (var read = host.CreateScope())
        {
            var users = await read.Db.Users.AsNoTracking().Select(u => new { u.Id, u.Email }).ToListAsync();
            var orders = await read.Db.Orders.AsNoTracking().Select(o => new { o.Id, o.UserId, o.Description }).ToListAsync();
            GraphAssert.UserPayloadMatches(
                rows.Single(r => r.Type == StoredTypeName.Of<UserCreated>()).Payload,
                users.Select(u => (u.Id, u.Email)).ToList(),
                orders.Select(o => (o.Id, o.UserId, o.Description)).ToList());
        }

        host.Database.Execute($"UPDATE {host.OutboxTable} SET {c.LockExpiresAt} = NULL");
        host.Faults.ClearLog();
        await using var claimScope = host.CreateScope();
        var store = Store(claimScope);
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var claimed = (await store.GetAllAsync(10, lockExpiration, 15)).ToList();
        var claim = Assert.Single(host.Faults.Commands, IsClaim);
        _output.WriteLine(claim.ToString());
        Assert.Contains($"SET {c.LockExpiresAt} = @lockExpiration", claim.Sql);
        Assert.Contains($"ORDER BY {c.Id}", claim.Sql);
        Assert.Contains($"{c.Attemps} IS NULL OR {c.Attemps} < @maxAttemps", claim.Sql);
        Assert.Contains($"{c.LockExpiresAt} IS NULL OR {c.LockExpiresAt} < @dateTimeNow", claim.Sql);
        Assert.Equal(rows.Select(r => Key(r.Id)), claimed.Select(m => m.Id));
        Assert.All(claimed, m => Assert.True(Math.Abs((m.LockExpiresAt!.Value - lockExpiration).TotalMilliseconds) < 4));
        Assert.All(claimed, m => Assert.NotNull(m.Type));
        Assert.Contains(claimed, m => m.Payload is not null && Json.Serialize(m.Payload).Contains("renamed {0} ]"));

        var failed = claimed[0];
        var failedLock = host.OutboxRows()[0].LockExpiresAt;
        failed.Attemps = 1;
        failed.IsError = true;
        failed.ErrorMessage = "failed: {0} ]";
        failed.LockExpiresAt = DateTime.UtcNow.AddYears(-1); // never written by UpdateAsync
        await store.UpdateAsync(failed);
        await store.RemoveAsync(claimed[1]);

        var after = Assert.Single(host.OutboxRows());
        Assert.Equal(failed.Id, Key(after.Id));
        Assert.Equal(1, after.Attemps);
        Assert.True(after.IsError);
        Assert.Equal("failed: {0} ]", after.ErrorMessage);
        Assert.Equal(failedLock, after.LockExpiresAt);
    }

    public static IEnumerable<object?[]> ClaimParameterCombinations()
    {
        foreach (var items in new int?[] { null, 2, 100 })
        foreach (var locks in new[] { false, true })
        foreach (var max in new int?[] { null, 3 })
            yield return new object?[] { items, locks, max };
    }

    /// <summary>Also proves that OUTPUT INSERTED.* / SELECT * materialize the renamed columns (Id, LockExpiresAt, Attemps).</summary>
    [SqlServerTheory]
    [MemberData(nameof(ClaimParameterCombinations))]
    public async Task GetAllAsync_EveryParameterCombination_ClaimsTheEligibleRowsAndMaterializesTheRenamedColumns(int? itemsCount, bool withLock, int? maxAttemps)
    {
        using var host = CreateHost();
        var now = DateTime.UtcNow;
        var free = Seed(host, null, null);
        var lockedFuture = Seed(host, now.AddMinutes(10), null);
        var twoAttempts = Seed(host, null, 2);
        var expired = Seed(host, now.AddMinutes(-10), 1);
        var threeAttempts = Seed(host, null, 3);
        var manyAttempts = Seed(host, now.AddMinutes(-10), 1000);
        var lockExpiration = withLock ? now.AddMinutes(30) : (DateTime?)null;

        var before = host.OutboxRows();
        var eligible = new HashSet<TKey> { free, twoAttempts, expired, threeAttempts, manyAttempts };
        if (maxAttemps != null)
        {
            eligible.Remove(threeAttempts);
            eligible.Remove(manyAttempts);
        }
        // In the database's Id order (uniqueidentifier order for Guid keys), as the claim's ORDER BY.
        var eligibleInOrder = before.Select(r => Key(r.Id)).Where(eligible.Contains).ToList();
        var expected = itemsCount.HasValue ? eligibleInOrder.Take(itemsCount.Value).ToList() : eligibleInOrder;

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(itemsCount, lockExpiration, maxAttemps)).ToList();
        var claim = Assert.Single(host.Faults.Commands);
        _output.WriteLine(claim.ToString());

        if (itemsCount.HasValue)
        {
            Assert.Equal(expected, claimed.Select(m => m.Id));
        }
        else
        {
            Assert.Equal(expected.OrderBy(x => x), claimed.Select(m => m.Id).OrderBy(x => x));
        }
        Assert.DoesNotContain(lockedFuture, claimed.Select(m => m.Id));
        Assert.Equal((itemsCount.HasValue ? 1 : 0) + (withLock ? 1 : 0) + (maxAttemps.HasValue ? 1 : 0) + 1, claim.Parameters.Count);

        var after = host.OutboxRows().ToDictionary(r => Key(r.Id));
        var beforeById = before.ToDictionary(r => Key(r.Id));
        foreach (var (id, row) in after)
        {
            if (withLock && expected.Contains(id))
            {
                Assert.True(Math.Abs((row.LockExpiresAt!.Value - lockExpiration!.Value).TotalMilliseconds) < 4, $"Row {id} lock not set by the claim.");
            }
            else
            {
                Assert.Equal(beforeById[id], row);
            }
        }

        // The returned entities carry the stored values of every renamed column.
        foreach (var message in claimed)
        {
            var row = after[message.Id];
            Assert.Equal(row.LockExpiresAt, message.LockExpiresAt);
            Assert.Equal(row.Attemps, message.Attemps);
            Assert.Equal(row.Type, message.Type);
            Assert.Equal(row.IsError, message.IsError);
            Assert.Equal(row.ErrorMessage, message.ErrorMessage);
            Assert.Equal(Json.Parse(row.Payload!).GetProperty("text").GetString(), Json.Parse(Json.Serialize(message.Payload)).GetProperty("text").GetString());
        }
    }

    [SqlServerFact]
    public async Task GetAllAsync_ConcurrentClaimers_NeverClaimTheSameRow()
    {
        using var host = CreateHost();
        var ids = Enumerable.Range(0, 60).Select(_ => Seed(host, null, null)).ToHashSet();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var start = new ManualResetEventSlim();

        var tasks = Enumerable.Range(0, 6).Select(_ => Task.Run(async () =>
        {
            await using var scope = host.CreateScope();
            await scope.Db.Database.OpenConnectionAsync();
            start.Wait();
            var claimed = new List<TKey>();
            for (var round = 0; round < 3; round++)
            {
                claimed.AddRange((await Store(scope).GetAllAsync(3, lockExpiration, 15)).Select(x => x.Id));
            }
            return claimed;
        })).ToList();
        start.Set();
        var all = (await Task.WhenAll(tasks)).SelectMany(x => x).ToList();

        Assert.Equal(54, all.Count);
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Subset(ids, all.ToHashSet());
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_RenewsMatchingRows_SkipsMismatchedAndDeletedRows()
    {
        using var host = CreateHost();
        var c = host.OutboxColumns;
        Enumerable.Range(0, 4).Select(_ => Seed(host, null, null)).ToList();
        var other = Seed(host, DateTime.UtcNow.AddMinutes(30), null);

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(4, DateTime.UtcNow.AddMinutes(5), 15)).ToList();
        Assert.Equal(4, claimed.Count);
        var expected = claimed.ToDictionary(m => m.Id, m => m.LockExpiresAt!.Value);
        expected[claimed[1].Id] = claimed[1].LockExpiresAt!.Value.AddSeconds(1); // stale expectation
        await Store(scope).RemoveAsync(claimed[2]);                                // deleted meanwhile
        var otherBefore = host.OutboxRows().Single(r => Key(r.Id).Equals(other)).LockExpiresAt;
        var newLock = PreciseLock(0);
        host.Faults.ClearLog();

        var renewed = await Store(scope).RenewLocksAsync(expected, newLock);

        var command = Assert.Single(host.Faults.Commands);
        _output.WriteLine(command.ToString());
        Assert.Contains($"SET {c.LockExpiresAt} = @lockExpiration", command.Sql);
        Assert.Contains($"OUTPUT INSERTED.{c.Id} AS [Id], INSERTED.{c.LockExpiresAt} AS [LockExpiresAt]", command.Sql);
        Assert.Contains($"ON t.{c.Id} = v.Id", command.Sql);
        Assert.Contains($"AND t.{c.LockExpiresAt} BETWEEN", command.Sql);
        Assert.DoesNotContain("READPAST", command.Sql);
        Assert.Equal(new[] { claimed[0].Id, claimed[3].Id }.OrderBy(x => x), renewed.Keys.OrderBy(x => x));
        Assert.All(renewed.Values, v => Assert.Equal(newLock, v));

        var rows = host.OutboxRows().ToDictionary(r => Key(r.Id));
        Assert.Equal(4, rows.Count);
        Assert.False(rows.ContainsKey(claimed[2].Id), "A renewal must never recreate a deleted row.");
        Assert.Equal(newLock, rows[claimed[0].Id].LockExpiresAt);
        Assert.Equal(newLock, rows[claimed[3].Id].LockExpiresAt);
        Assert.Equal(claimed[1].LockExpiresAt, rows[claimed[1].Id].LockExpiresAt);
        Assert.Equal(otherBefore, rows[other].LockExpiresAt);
        Assert.Empty(scope.Db.ChangeTracker.Entries());
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_MoreThanOneThousandIds_RenewsAllInChunks()
    {
        using var host = CreateHost();
        SeedMany(host, 2150);

        await using var scope = host.CreateScope();
        var claimed = (await Store(scope).GetAllAsync(null, DateTime.UtcNow.AddMinutes(5), null)).ToList();
        Assert.Equal(2150, claimed.Count);
        var newLock = PreciseLock(0);
        host.Faults.ClearLog();

        var renewed = await Store(scope).RenewLocksAsync(claimed.ToDictionary(m => m.Id, m => m.LockExpiresAt!.Value), newLock);

        Assert.Equal(claimed.Select(m => m.Id).OrderBy(x => x), renewed.Keys.OrderBy(x => x));
        Assert.All(host.OutboxRows(), r => Assert.Equal(newLock, r.LockExpiresAt));
        var commands = host.Faults.Commands;
        Assert.Equal(3, commands.Count);
        Assert.All(commands, c => Assert.True(c.Parameters.Count <= 1 + 2 * 1000, $"{c.Parameters.Count} parameters"));
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_RacingAClaimOnAnExpiredRow_ExactlyOneWins()
    {
        using var host = CreateHost();
        int renewalWins = 0, claimWins = 0;
        for (var round = 0; round < 10; round++)
        {
            host.Database.Execute($"DELETE FROM {host.OutboxTable}");
            var id = Seed(host, null, null);
            await using var owner = host.CreateScope();
            var owned = (await Store(owner).GetAllAsync(1, DateTime.UtcNow.AddSeconds(-30), 15)).Single(); // expired lock
            await using var claimerScope = host.CreateScope();
            await claimerScope.Db.Database.OpenConnectionAsync();
            await owner.Db.Database.OpenConnectionAsync();
            var start = new ManualResetEventSlim();

            var renewal = Task.Run(async () =>
            {
                start.Wait();
                return await Store(owner).RenewLocksAsync(new Dictionary<TKey, DateTime> { [id] = owned.LockExpiresAt!.Value }, DateTime.UtcNow.AddMinutes(10));
            });
            var claim = Task.Run(async () =>
            {
                start.Wait();
                return (await Store(claimerScope).GetAllAsync(1, DateTime.UtcNow.AddMinutes(20), 15)).ToList();
            });
            start.Set();
            var renewed = await renewal;
            var claimed = await claim;

            var stored = Assert.Single(host.OutboxRows()).LockExpiresAt!.Value;
            if (renewed.ContainsKey(id))
            {
                renewalWins++;
                Assert.Empty(claimed);
                Assert.Equal(renewed[id], stored);
            }
            else
            {
                claimWins++;
                Assert.Equal(id, Assert.Single(claimed).Id);
                Assert.Equal(Assert.Single(claimed).LockExpiresAt, stored);
            }
        }
        _output.WriteLine($"Renewal won {renewalWins}, claim won {claimWins}");
    }

    private static async Task WaitUntil(Func<bool> condition, string what, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(20));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("Timed out waiting for: " + what);
            await Task.Delay(50);
        }
    }

    private static async Task<List<IHostedService>> Start(SmokeHost<TKey, TContext> host)
    {
        var services = host.Services.GetServices<IHostedService>().ToList();
        Assert.Single(services, s => s is OutboxProcessorHostedService<TKey>);
        foreach (var service in services) await service.StartAsync(CancellationToken.None);
        return services;
    }

    private static async Task Stop(List<IHostedService> services)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        foreach (var service in services) await service.StopAsync(cts.Token);
    }

    [SqlServerFact]
    public async Task HostedService_PollerAndImmediateDispatch_InvokeTheSubscriberOnceAndDeleteTheRows()
    {
        using var host = CreateHost(new SmokeHostOptions { RealQueue = true });
        Seed(host, null, null, "{\"text\":\"from the poller\",\"number\":42}");
        var probe = host.Services.GetRequiredService<SubscriberProbe>();

        var services = await Start(host);
        try
        {
            await WaitUntil(() => probe.Received.Count >= 1 && host.OutboxRows().Count == 0, "poller dispatch and row deleted");
            Assert.Contains(host.Faults.Commands, IsClaim);

            await using (var scope = host.CreateScope())
            {
                scope.Db.Outbox.AddEvent(new ProbeEvent { Text = "immediate", Number = 7 });
                await scope.Db.SaveChangesAsync();
            }
            await WaitUntil(() => probe.Received.Count >= 2 && host.OutboxRows().Count == 0, "immediate dispatch and row deleted");
            await Task.Delay(300);

            Assert.Equal(new[] { "from the poller", "immediate" }, probe.Received.Select(r => r.Text));
            Assert.Empty(host.OutboxRows());
        }
        finally
        {
            await Stop(services);
        }
    }

    [SqlServerFact]
    public async Task HostedService_SubscriberThrows_RecordsTheFailedAttemptInTheRenamedColumns()
    {
        using var host = CreateHost(new SmokeHostOptions { RealQueue = true });
        var services = await Start(host);
        try
        {
            await using (var scope = host.CreateScope())
            {
                scope.Db.Outbox.AddEvent(new AsyncFailingEvent { Text = "boom" });
                await scope.Db.SaveChangesAsync();
            }
            var lockAfterSave = Assert.Single(host.OutboxRows()).LockExpiresAt;

            await WaitUntil(() => host.OutboxRows().SingleOrDefault()?.Attemps == 1, "failed attempt recorded");

            var row = Assert.Single(host.OutboxRows());
            Assert.True(row.IsError);
            Assert.Equal(lockAfterSave, row.LockExpiresAt);
            Assert.Equal("subscriber failed on purpose", row.ErrorMessage);
        }
        finally
        {
            await Stop(services);
        }
    }

    [SqlServerFact]
    public async Task HostedService_HandlerOutlastingTheLock_IsRenewedAndNotClaimedByACompetingPoller()
    {
        var span = TimeSpan.FromSeconds(2);
        var probe = new SlowProbe { Duration = span * 3 };
        using var host = CreateHost(new SmokeHostOptions { RealQueue = true }, services =>
        {
            services.AddSingleton(probe);
            services.Configure<OutboxConfiguration>(o => o.MessageLockExpirationTimeSpan = span);
        });

        var hosted = await Start(host);
        var competitorClaims = new List<TKey>();
        DateTime? firstLock = null, latestLock = null;
        try
        {
            await using (var save = host.CreateScope())
            {
                save.Db.Outbox.AddEvent(new SlowEvent { Text = "slow" });
                await save.Db.SaveChangesAsync();
            }
            firstLock = host.OutboxRows().SingleOrDefault()?.LockExpiresAt;

            await WaitUntil(() => probe.Invocations == 1, "handler started");

            // Another instance's poller: claims every row whose lock has expired, a few times per second.
            var watch = Stopwatch.StartNew();
            while (probe.Completed == 0 || host.OutboxRows().Count > 0)
            {
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(30), "The handler did not finish and delete the row in time.");
                latestLock = host.OutboxRows().SingleOrDefault()?.LockExpiresAt ?? latestLock;
                await using (var competitor = host.CreateScope())
                {
                    var claimed = await Store(competitor).GetAllAsync(101, DateTime.UtcNow.AddMinutes(10), 15);
                    competitorClaims.AddRange(claimed.Select(m => m.Id));
                }
                if (competitorClaims.Count > 0) break;
                await Task.Delay(200);
            }
            await Task.Delay(500);
        }
        finally
        {
            await Stop(hosted);
        }

        _output.WriteLine($"Lock after save {firstLock:O}, latest observed {latestLock:O}");
        Assert.Empty(competitorClaims);
        Assert.Equal(1, probe.Invocations);
        Assert.Equal(1, probe.Completed);
        Assert.Empty(host.OutboxRows());
        Assert.Contains(host.Faults.Commands, IsRenewal);
        Assert.NotNull(firstLock);
        Assert.True(latestLock > firstLock, "The lock was never renewed while the handler ran.");
    }
}

public class HostileColumnsScenarioTests : RenamedColumnScenarioTests<int, HostileColumnsDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public HostileColumnsScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => OutboxColumnNames.Hostile;
}

public class UnicodeColumnsScenarioTests : RenamedColumnScenarioTests<int, UnicodeColumnsDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public UnicodeColumnsScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => OutboxColumnNames.Unicode;
}

public class SnakeCaseColumnsScenarioTests : RenamedColumnScenarioTests<int, SnakeCaseColumnsDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public SnakeCaseColumnsScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => OutboxColumnNames.SnakeCase;
}

public class SnakeCasePluginScenarioTests : RenamedColumnScenarioTests<int, SnakeCasePluginDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public SnakeCasePluginScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => OutboxColumnNames.SnakeCase;
}

public class GuidHostileColumnsScenarioTests : RenamedColumnScenarioTests<Guid, GuidHostileColumnsDbContext>, IClassFixture<SmokeDatabaseFixture>
{
    public GuidHostileColumnsScenarioTests(SmokeDatabaseFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    protected override OutboxColumnNames ExpectedNames => OutboxColumnNames.Hostile;
}

/// <summary>
/// LockExpiresAt renamed only for the outbox table with a table-specific override,
/// <c>ToTable("OutboxMessages", t =&gt; t.Property(x =&gt; x.LockExpiresAt).HasColumnName(...))</c>, instead of HasColumnName on the
/// property.
/// </summary>
public sealed class TableSpecificColumnOverrideDbContext : SmokeContextBase<int>
{
    public const string LockColumn = "lock_override";

    public TableSpecificColumnOverrideDbContext(DbContextOptions<TableSpecificColumnOverrideDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable("OutboxMessages", t => t.Property(x => x.LockExpiresAt).HasColumnName(LockColumn));
}

/// <summary>LockExpiresAt moved to a second table with entity splitting: it is not a column of the outbox table.</summary>
public sealed class SplitLockColumnDbContext : SmokeContextBase<int>
{
    public SplitLockColumnDbContext(DbContextOptions<SplitLockColumnDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().SplitToTable("OutboxLocks", t => t.Property(x => x.LockExpiresAt));
}

/// <summary>The outbox entity mapped to a view only (no table).</summary>
public sealed class ViewMappedOutboxDbContext : SmokeContextBase<int>
{
    public ViewMappedOutboxDbContext(DbContextOptions<ViewMappedOutboxDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable((string?)null).ToView("OutboxMessagesView");
}

/// <summary>
/// Not supported, rejected: a table-specific column override. EF materializes the claim's FromSql result by the
/// property's default column name, so the claim throws a clear InvalidOperationException before sending any SQL (it
/// would otherwise lock rows it cannot read); the renewal (its own result type and aliases) works.
/// </summary>
public class TableSpecificColumnOverrideTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TableSpecificColumnOverrideTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [SqlServerTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GetAllAsync_LockColumnRenamedOnlyForTheTable_ThrowsBeforeSendingTheClaim(bool withLock)
    {
        using var host = SmokeHost<int, TableSpecificColumnOverrideDbContext>.Create(_fixture);
        Assert.Equal("[lock_override]", host.OutboxColumns.LockExpiresAt);
        await using (var scope = host.CreateScope())
        {
            Assert.Equal("LockExpiresAt", scope.Db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindProperty("LockExpiresAt")!.GetColumnName());
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "override" });
            await scope.Db.SaveChangesAsync(); // EF's INSERT uses the table's column
        }
        host.Database.Execute($"UPDATE {host.OutboxTable} SET {host.OutboxColumns.LockExpiresAt} = NULL");
        host.Faults.ClearLog();

        await using var claimScope = host.CreateScope();
        var lockExpiration = DateTime.UtcNow.AddMinutes(5);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => claimScope.Services.GetRequiredService<IOutboxStore<int>>()
            .GetAllAsync(10, withLock ? lockExpiration : null, 15));
        _output.WriteLine(ex.Message);
        Assert.Contains("table-specific column overrides", ex.Message);
        Assert.Contains("OutboxMessage<int>.LockExpiresAt", ex.Message);
        Assert.Contains("'lock_override'", ex.Message);
        Assert.Contains("HasColumnName(\"lock_override\")", ex.Message);

        // Rejected before any command: the row is neither claimed nor hidden.
        Assert.Empty(host.Faults.Commands);
        var row = Assert.Single(host.OutboxRows());
        Assert.Null(row.LockExpiresAt);

        // Repeated polls behave the same and still leave the row untouched.
        await Assert.ThrowsAsync<InvalidOperationException>(() => claimScope.Services.GetRequiredService<IOutboxStore<int>>()
            .GetAllAsync(10, withLock ? lockExpiration : null, 15));
        Assert.Empty(host.Faults.Commands);
        Assert.Null(Assert.Single(host.OutboxRows()).LockExpiresAt);
    }

    [SqlServerFact]
    public async Task RenewLocksAsync_LockColumnRenamedOnlyForTheTable_RenewsTheTableColumn()
    {
        using var host = SmokeHost<int, TableSpecificColumnOverrideDbContext>.Create(_fixture);
        var stored = DateTime.UtcNow.AddMinutes(1);
        var id = StoreSeed.Row<int>(host.Database, host.OutboxTable, host.OutboxColumns, null, null);
        host.Database.Execute($"UPDATE {host.OutboxTable} SET {host.OutboxColumns.LockExpiresAt} = CONVERT(datetime2, @lock, 126)",
            ("@lock", stored.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")));
        var newLock = new DateTime(2099, 1, 1).AddTicks(1234567);

        await using var scope = host.CreateScope();
        var renewed = await scope.Services.GetRequiredService<IOutboxStore<int>>()
            .RenewLocksAsync(new Dictionary<int, DateTime> { [id] = stored }, newLock);

        Assert.Equal(newLock, renewed[id]);
        Assert.Equal(newLock, Assert.Single(host.OutboxRows()).LockExpiresAt);
    }
}

/// <summary>Mappings the claim and renewal cannot target fail with a clear InvalidOperationException before any command. Model only: no database.</summary>
public class UnsupportedOutboxMappingTests
{
    private static async Task<InvalidOperationException> Throws<TContext>(Func<IOutboxStore<int>, Task> action) where TContext : SmokeContextBase<int>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        // Never opened: the store must fail before it sends anything.
        services.AddDbContext<TContext>(o => o
            .UseSqlServer(@"Server=(localdb)\MSSQLLocalDB;Database=XDevOutboxModelOnly_NotCreated;Connect Timeout=1")
            .ConfigureWarnings(w => w.Ignore(Microsoft.EntityFrameworkCore.Diagnostics.CoreEventId.ManyServiceProvidersCreatedWarning)));
        services.AddOutbox<int>(typeof(SmokeHost<,>).Assembly);
        services.AddEFCoreOutboxStore<int, TContext>();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        return await Assert.ThrowsAsync<InvalidOperationException>(() => action(scope.ServiceProvider.GetRequiredService<IOutboxStore<int>>()));
    }

    private static Task Claim(IOutboxStore<int> store) => store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15);

    private static Task Renew(IOutboxStore<int> store) => store.RenewLocksAsync(new Dictionary<int, DateTime> { [1] = DateTime.UtcNow }, DateTime.UtcNow.AddMinutes(5));

    [Fact]
    public async Task GetAllAsync_LockColumnInASplitTable_ThrowsNamingThePropertyAndTheTable()
    {
        var ex = await Throws<SplitLockColumnDbContext>(Claim);
        Assert.Contains("OutboxMessage<int>.LockExpiresAt", ex.Message);
        Assert.Contains("'OutboxMessages'", ex.Message);
        Assert.Contains("entity splitting", ex.Message);
    }

    [Fact]
    public async Task RenewLocksAsync_LockColumnInASplitTable_ThrowsNamingThePropertyAndTheTable()
    {
        var ex = await Throws<SplitLockColumnDbContext>(Renew);
        Assert.Contains("OutboxMessage<int>.LockExpiresAt", ex.Message);
        Assert.Contains("entity splitting", ex.Message);
    }

    [Fact]
    public async Task GetAllAsync_OutboxMappedToAView_ThrowsNotMappedToATable()
    {
        var ex = await Throws<ViewMappedOutboxDbContext>(Claim);
        Assert.Contains("not mapped to a table", ex.Message);
    }

    [Fact]
    public async Task RenewLocksAsync_OutboxMappedToAView_ThrowsNotMappedToATable()
    {
        var ex = await Throws<ViewMappedOutboxDbContext>(Renew);
        Assert.Contains("not mapped to a table", ex.Message);
    }
}
