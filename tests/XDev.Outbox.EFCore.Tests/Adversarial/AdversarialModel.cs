using System.ComponentModel;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>Entity whose Doubled column is computed by the database on every insert and update.</summary>
public class Product
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Price { get; set; }
    public int Doubled { get; set; }
}

/// <summary>Client-generated Guid key (never store-generated).</summary>
public class Doc
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
}

/// <summary>Composite, caller-assigned key.</summary>
public class Line
{
    public int OrderNo { get; set; }
    public int LineNo { get; set; }
    public string Sku { get; set; } = "";
}

public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public Address Address { get; set; } = new();
}

public class Address
{
    public string City { get; set; } = "";
}

/// <summary>
/// Full-notification entity whose backing fields are not discoverable by EF conventions, so EF (and the
/// payload projection) must go through the notifying property setters.
/// </summary>
public class NotifyItem : INotifyPropertyChanged, INotifyPropertyChanging
{
    private int _storageForKey;
    private string _storageForLabel = "";

    public event PropertyChangedEventHandler? PropertyChanged;
    public event PropertyChangingEventHandler? PropertyChanging;

    public int Id
    {
        get => _storageForKey;
        set
        {
            if (_storageForKey == value) return;
            PropertyChanging?.Invoke(this, new PropertyChangingEventArgs(nameof(Id)));
            _storageForKey = value;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Id)));
        }
    }

    public string Label
    {
        get => _storageForLabel;
        set
        {
            if (_storageForLabel == value) return;
            PropertyChanging?.Invoke(this, new PropertyChangingEventArgs(nameof(Label)));
            _storageForLabel = value;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Label)));
        }
    }
}

/// <summary>Key setter with an observable side effect and no conventionally named backing field.</summary>
public class SideEffectItem
{
    private int _storageForKey;

    public int Id
    {
        get => _storageForKey;
        set
        {
            _storageForKey = value;
            KeySetterCalls++;
        }
    }

    public string Name { get; set; } = "";

    public int KeySetterCalls { get; private set; }
}

public class ProductChanged { public Product? Product { get; set; } }
public class DocCreated { public Doc? Doc { get; set; } }
public class LineAdded { public Line? Line { get; set; } }
public class CustomerCreated { public Customer? Customer { get; set; } }
public class NotifyItemCreated { public NotifyItem? Item { get; set; } }
public class SideEffectItemCreated { public SideEffectItem? Item { get; set; } }
public class PlainEvent { public string Text { get; set; } = ""; }
public class UsersImported { public List<User> Users { get; set; } = new(); }
public class Envelope { public Wrapper Inner { get; set; } = new(); }
public class Wrapper { public User? User { get; set; } }

/// <summary>Getter throws once the referenced user carries a non-default key: serialization succeeds for the INSERT, fails for the payload fix-up.</summary>
public class ExplodingAfterKeyEvent
{
    public User? User { get; set; }
    public int Probe => User != null && User.Id != 0 ? throw new InvalidOperationException("payload serialization failure") : 0;
}

public class AdvDbContext : OutboxDbContext<int>
{
    public AdvDbContext(DbContextOptions<AdvDbContext> options) : base(options) { }

    public DbSet<User> Users => Set<User>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Doc> Docs => Set<Doc>();
    public DbSet<Line> Lines => Set<Line>();
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<NotifyItem> NotifyItems => Set<NotifyItem>();
    public DbSet<SideEffectItem> SideEffectItems => Set<SideEffectItem>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.Entity<User>(b =>
        {
            b.ToTable("Users");
            b.Property(x => x.Id).ValueGeneratedOnAdd();
            b.HasMany(x => x.Orders).WithOne(x => x.User).HasForeignKey(x => x.UserId);
        });
        builder.Entity<Order>(b => b.ToTable("Orders"));
        builder.Entity<Product>(b =>
        {
            b.ToTable("Products");
            b.Property(x => x.Doubled).HasComputedColumnSql("\"Price\" * 2", stored: true);
        });
        builder.Entity<Doc>(b => b.ToTable("Docs"));
        builder.Entity<Line>(b =>
        {
            b.ToTable("Lines");
            b.HasKey(x => new { x.OrderNo, x.LineNo });
        });
        builder.Entity<Customer>(b =>
        {
            b.ToTable("Customers");
            b.OwnsOne(x => x.Address);
        });
        builder.Entity<NotifyItem>(b =>
        {
            b.ToTable("NotifyItems");
            b.HasChangeTrackingStrategy(ChangeTrackingStrategy.ChangingAndChangedNotifications);
            b.Property(x => x.Id).UsePropertyAccessMode(PropertyAccessMode.Property);
            b.Property(x => x.Label).UsePropertyAccessMode(PropertyAccessMode.Property);
        });
        builder.Entity<SideEffectItem>(b =>
        {
            b.ToTable("SideEffectItems");
            b.Property(x => x.Id).UsePropertyAccessMode(PropertyAccessMode.Property);
            b.Ignore(x => x.KeySetterCalls);
        });

        base.OnModelCreating(builder);
    }
}

/// <summary>Outbox table renamed through AddEFCoreOutboxStore(o => o.TableName = ...).</summary>
public class CustomTableDbContext : OutboxDbContext<int>
{
    public const string Table = "Custom Outbox Table";

    public CustomTableDbContext(DbContextOptions<CustomTableDbContext> options) : base(options) { }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.Entity<User>(b => { b.ToTable("Users"); b.Ignore(x => x.Orders); });
        base.OnModelCreating(builder);
    }
}

/// <summary>Outbox table name containing composite-format braces.</summary>
public class BraceTableDbContext : OutboxDbContext<int>
{
    public const string Table = "Outbox{1}";

    public BraceTableDbContext(DbContextOptions<BraceTableDbContext> options) : base(options) { }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.Entity<User>(b => { b.ToTable("Users"); b.Ignore(x => x.Orders); });
        base.OnModelCreating(builder);
    }
}

/// <summary>Outbox with a Guid key (AddOutbox&lt;Guid&gt; / AddEFCoreOutboxStore&lt;Guid, TContext&gt;).</summary>
public class GuidOutboxDbContext : OutboxDbContext<Guid>
{
    public GuidOutboxDbContext(DbContextOptions<GuidOutboxDbContext> options) : base(options) { }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.Entity<User>(b => { b.ToTable("Users"); b.Ignore(x => x.Orders); });
        base.OnModelCreating(builder);
    }
}

public sealed class RecordingQueue<TKey> : IMessageQueue<TKey> where TKey : struct
{
    private readonly object _gate = new();
    private readonly List<OutboxMessage<TKey>> _items = new();

    public IReadOnlyList<OutboxMessage<TKey>> Enqueued { get { lock (_gate) return _items.ToArray(); } }

    public Task EnqueueAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
    {
        lock (_gate) _items.Add(message);
        return Task.CompletedTask;
    }

    public event Func<OutboxMessage<TKey>, CancellationToken, Task>? MessageReceived { add { } remove { } }
    public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}

/// <summary>Isolated SQLite in-memory host for an arbitrary outbox context and key type.</summary>
public sealed class AdvHost<TContext, TKey> : IDisposable
    where TContext : DbContext, IOutboxDbContext<TKey>
    where TKey : struct
{
    private readonly string _outboxTable;

    private AdvHost(SqliteConnection connection, ServiceProvider services, RecordingQueue<TKey> queue, FaultInjector faults, string outboxTable)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
        _outboxTable = outboxTable;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingQueue<TKey> Queue { get; }
    public FaultInjector Faults { get; }

    public static AdvHost<TContext, TKey> Create(string outboxTable = "OutboxMessages", Action<OutboxDbContextConfiguration>? configure = null)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingQueue<TKey>();
        var faults = new FaultInjector();

        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<TKey>>(queue);
        services.AddDbContext<TContext>(options =>
        {
            options.UseSqlite(connection);
            options.AddInterceptors(new FaultCommandInterceptor(faults), new FaultCommitInterceptor(faults));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        });
        services.AddOutbox<TKey>(typeof(AdvHost<,>).Assembly);
        services.AddEFCoreOutboxStore<TKey, TContext>(configure);

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using (var scope = provider.CreateScope())
        {
            scope.ServiceProvider.GetRequiredService<TContext>().Database.EnsureCreated();
        }
        return new AdvHost<TContext, TKey>(connection, provider, queue, faults, outboxTable);
    }

    public (IServiceScope Scope, TContext Db, IOutbox Outbox) CreateScope()
    {
        var scope = Services.CreateScope();
        return (scope, scope.ServiceProvider.GetRequiredService<TContext>(), scope.ServiceProvider.GetRequiredService<IOutbox>());
    }

    public long Scalar(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }

    public void Exec(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    public List<(string Id, string Payload)> OutboxRows()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = $"SELECT \"Id\", \"Payload\" FROM \"{_outboxTable}\" ORDER BY rowid";
        using var reader = command.ExecuteReader();
        var rows = new List<(string, string)>();
        while (reader.Read()) rows.Add((Convert.ToString(reader.GetValue(0))!, reader.GetString(1)));
        return rows;
    }

    public int OutboxUpdateCommands()
        => Faults.ExecutedCommands.Count(c => c.TrimStart().StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase) && c.Contains(_outboxTable, StringComparison.Ordinal));

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}
