using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.TypeNames;

/// <summary>The consumer configures the Type column with HasColumnType only (no HasMaxLength) after base.OnModelCreating.</summary>
public abstract class TypeColumnTypeDbContextBase : OutboxDbContext<int>
{
    protected TypeColumnTypeDbContextBase(DbContextOptions options) : base(options)
    {
    }

    public DbSet<User> Users => Set<User>();

    protected abstract string ColumnType { get; }

    protected override void OnModelCreating(ModelBuilder builder)
    {
        TypeNameModel.Configure(builder);
        base.OnModelCreating(builder);
        builder.Entity<OutboxMessage<int>>().Property(x => x.Type).HasColumnType(ColumnType);
    }
}

public sealed class NVarChar500TypeColumnDbContext(DbContextOptions<NVarChar500TypeColumnDbContext> options) : TypeColumnTypeDbContextBase(options)
{
    protected override string ColumnType => "nvarchar(500)";
}

public sealed class NVarCharMaxTypeColumnDbContext(DbContextOptions<NVarCharMaxTypeColumnDbContext> options) : TypeColumnTypeDbContextBase(options)
{
    protected override string ColumnType => "NVARCHAR(MAX)";
}

public sealed class TextTypeColumnDbContext(DbContextOptions<TextTypeColumnDbContext> options) : TypeColumnTypeDbContextBase(options)
{
    protected override string ColumnType => "TEXT";
}

public sealed class VarChar250TypeColumnDbContext(DbContextOptions<VarChar250TypeColumnDbContext> options) : TypeColumnTypeDbContextBase(options)
{
    protected override string ColumnType => "varchar (250)";
}

/// <summary>A column type whose length cannot be read: the model's max length (200) applies.</summary>
public sealed class UnreadableLengthTypeColumnDbContext(DbContextOptions<UnreadableLengthTypeColumnDbContext> options) : TypeColumnTypeDbContextBase(options)
{
    protected override string ColumnType => "nvarchar(500, 1)";
}

/// <summary>A character type without a length is not unlimited (SQL Server: nvarchar(1)): the model's max length (200) applies.</summary>
public sealed class NVarCharWithoutLengthTypeColumnDbContext(DbContextOptions<NVarCharWithoutLengthTypeColumnDbContext> options) : TypeColumnTypeDbContextBase(options)
{
    protected override string ColumnType => "nvarchar";
}

/// <summary>sysname (SQL Server: nvarchar(128)) has no length to read: the model's max length (200) applies.</summary>
public sealed class SysNameTypeColumnDbContext(DbContextOptions<SysNameTypeColumnDbContext> options) : TypeColumnTypeDbContextBase(options)
{
    protected override string ColumnType => "sysname";
}

/// <summary>
/// The length check uses the column the Type property is mapped to: an explicit column type (HasColumnType) wins over
/// the model's max length (200 by default), like the column EF creates; without a readable length in it, the max length
/// applies. SQLite: the check is the only limit.
/// </summary>
public class EventTypeNameColumnTypeTests
{
    // Version-less, still longer than 250 and shorter than 500 characters.
    private static readonly Type LongType = typeof(EntityChanged<Dictionary<string, List<KeyValuePair<int, Guid>>>>);

    private readonly ITestOutputHelper _output;

    public EventTypeNameColumnTypeTests(ITestOutputHelper output) => _output = output;

    private sealed class Host<TContext> : IDisposable where TContext : OutboxDbContext<int>
    {
        private readonly SqliteConnection _connection;
        private readonly ServiceProvider _provider;
        private readonly IServiceScope _scope;

        public Host(bool createSchema)
        {
            _connection = new SqliteConnection("Data Source=:memory:");
            _connection.Open();
            var services = new ServiceCollection();
            services.AddSingleton<IMessageQueue<int>>(Queue);
            services.AddDbContext<TContext>(options =>
            {
                options.UseSqlite(_connection);
                options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
                options.AddInterceptors(Commands);
            });
            services.AddOutbox(typeof(EventTypeNameColumnTypeTests).Assembly);
            services.AddEFCoreOutboxStore<TContext>();
            _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
            _scope = _provider.CreateScope();
            Db = _scope.ServiceProvider.GetRequiredService<TContext>();
            if (createSchema)
            {
                // SQLite does not parse "(MAX)": create the same schema with TEXT for it (the model keeps its column type).
                using var command = _connection.CreateCommand();
                command.CommandText = Db.Database.GenerateCreateScript().Replace("NVARCHAR(MAX)", "TEXT");
                command.ExecuteNonQuery();
            }
            Commands.Executed.Clear();
        }

        public RecordingMessageQueue Queue { get; } = new();

        public CommandLog Commands { get; } = new();

        public TContext Db { get; }

        public string? StoredType()
        {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT \"Type\" FROM \"OutboxMessages\"";
            return (string?)command.ExecuteScalar();
        }

        public void Dispose()
        {
            _scope.Dispose();
            _provider.Dispose();
            _connection.Dispose();
        }
    }

    public sealed class CommandLog : DbCommandInterceptor
    {
        public List<string> Executed { get; } = new();

        public override InterceptionResult<int> NonQueryExecuting(System.Data.Common.DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
        {
            Executed.Add(command.CommandText);
            return result;
        }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(System.Data.Common.DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            Executed.Add(command.CommandText);
            return ValueTask.FromResult(result);
        }

        public override InterceptionResult<System.Data.Common.DbDataReader> ReaderExecuting(System.Data.Common.DbCommand command, CommandEventData eventData, InterceptionResult<System.Data.Common.DbDataReader> result)
        {
            Executed.Add(command.CommandText);
            return result;
        }

        public override ValueTask<InterceptionResult<System.Data.Common.DbDataReader>> ReaderExecutingAsync(System.Data.Common.DbCommand command, CommandEventData eventData, InterceptionResult<System.Data.Common.DbDataReader> result, CancellationToken cancellationToken = default)
        {
            Executed.Add(command.CommandText);
            return ValueTask.FromResult(result);
        }
    }

    private async Task Saves<TContext>(bool async) where TContext : OutboxDbContext<int>
    {
        var name = OutboxTypeName.Get(LongType);
        _output.WriteLine($"{name.Length}: {name}");
        Assert.InRange(name.Length, 251, 500);

        using var host = new Host<TContext>(createSchema: true);
        Assert.Equal(200, host.Db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindProperty(nameof(OutboxMessage<int>.Type))!.GetMaxLength());
        host.Db.Outbox.AddEvent(Activator.CreateInstance(LongType)!);

        await Save.Invoke(host.Db, async);

        Assert.Equal(name, host.StoredType());
        Assert.Single(host.Queue.Enqueued);
    }

    private async Task<InvalidOperationException> Rejects<TContext>(bool async) where TContext : OutboxDbContext<int>
    {
        using var host = new Host<TContext>(createSchema: false);
        var @event = Activator.CreateInstance(LongType)!;
        host.Db.Outbox.AddEvent(@event);

        var error = await Record.ExceptionAsync(() => Save.Invoke(host.Db, async));

        var invalid = Assert.IsType<InvalidOperationException>(error);
        _output.WriteLine(invalid.Message);
        Assert.Empty(host.Commands.Executed);
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(new[] { @event }, host.Db.Outbox.GetEvents());
        return invalid;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Save_ColumnTypeNVarChar500WithoutMaxLength_NameOver200IsSaved(bool async) => Saves<NVarChar500TypeColumnDbContext>(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Save_ColumnTypeMax_NoLimit(bool async) => Saves<NVarCharMaxTypeColumnDbContext>(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Save_ColumnTypeWithoutLength_NoLimit(bool async) => Saves<TextTypeColumnDbContext>(async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_ColumnTypeVarChar250_NameOver250IsRejectedNamingTheColumnType(bool async)
    {
        var invalid = await Rejects<VarChar250TypeColumnDbContext>(async);
        Assert.Contains("allows at most 250 (column type 'varchar (250)')", invalid.Message);
        Assert.Contains("OutboxMessage<int>", invalid.Message);
        Assert.Contains(".Property(x => x.Type).HasColumnType(", invalid.Message);
        Assert.DoesNotContain("HasMaxLength", invalid.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_ColumnTypeLengthUnreadable_TheModelMaxLengthApplies(bool async)
    {
        var invalid = await Rejects<UnreadableLengthTypeColumnDbContext>(async);
        Assert.Contains("allows at most 200. Nothing was saved.", invalid.Message);
        Assert.Contains(".Property(x => x.Type).HasMaxLength(n)", invalid.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_ColumnTypeNVarCharWithoutLength_TheModelMaxLengthApplies(bool async)
    {
        var invalid = await Rejects<NVarCharWithoutLengthTypeColumnDbContext>(async);
        Assert.Contains("allows at most 200. Nothing was saved.", invalid.Message);
        Assert.Contains(".Property(x => x.Type).HasMaxLength(n)", invalid.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_ColumnTypeSysName_TheModelMaxLengthApplies(bool async)
    {
        var invalid = await Rejects<SysNameTypeColumnDbContext>(async);
        Assert.Contains("allows at most 200. Nothing was saved.", invalid.Message);
        Assert.Contains(".Property(x => x.Type).HasMaxLength(n)", invalid.Message);
    }
}
