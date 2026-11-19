using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Round2;

// ---- Deep graph: Company -> Dept -> Employee (self-referencing manager), Company -> Folder (Guid key) -> FileItem ----

public class Company
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public List<Dept> Depts { get; set; } = new();
    public List<Folder> Folders { get; set; } = new();
}

public class Dept
{
    public int Id { get; set; }
    public int CompanyId { get; set; }
    public string Name { get; set; } = "";
    public List<Employee> Employees { get; set; } = new();
}

public class Employee
{
    public int Id { get; set; }
    public int DeptId { get; set; }
    public int? ManagerId { get; set; }
    [JsonIgnore] public Employee? Manager { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>Client-generated Guid key whose owner has a store-generated key.</summary>
public class Folder
{
    public Guid Id { get; set; }
    public int CompanyId { get; set; }
    public string Name { get; set; } = "";
    public List<FileItem> Files { get; set; } = new();
}

public class FileItem
{
    public int Id { get; set; }
    public Guid FolderId { get; set; }
    public string Name { get; set; } = "";
}

// ---- Many-to-many (unidirectional skip navigation, property-bag join entity) ----

public class Post
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public List<Tag> Tags { get; set; } = new();
}

public class Tag
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

// ---- Owned collection with its own store-generated key ----

public class Shipment
{
    public int Id { get; set; }
    public string Ref { get; set; } = "";
    public List<Parcel> Parcels { get; set; } = new();
}

public class Parcel
{
    public int Id { get; set; }
    public string Label { get; set; } = "";
}

/// <summary>
/// Key without a backing field EF can discover (setter fallback). The setter can be told to reject non-positive
/// values (a domain guard) either always or only for the next N calls.
/// </summary>
public class GuardedItem
{
    private int _storageForKey;

    public int Id
    {
        get => _storageForKey;
        set
        {
            if (value <= 0 && (RejectNonPositive || RejectNonPositiveRemaining > 0))
            {
                if (RejectNonPositiveRemaining > 0) RejectNonPositiveRemaining--;
                throw new ArgumentOutOfRangeException(nameof(value), value, "Id must be positive.");
            }
            _storageForKey = value;
        }
    }

    public string Name { get; set; } = "";

    [JsonIgnore] public bool RejectNonPositive { get; set; }
    [JsonIgnore] public int RejectNonPositiveRemaining { get; set; }
}

public class CompanyCreated { public Company? Company { get; set; } }
public class PostCreated { public Post? Post { get; set; } }
public class ShipmentCreated { public Shipment? Shipment { get; set; } }
public class GuardedCreated { public GuardedItem? Item { get; set; } public User? User { get; set; } }
public class NotifyItemsCreated { public List<NotifyItem> Items { get; set; } = new(); }
public class SideEffectItemsCreated { public List<SideEffectItem> Items { get; set; } = new(); }

public class R2DbContext : OutboxDbContext<int>
{
    public R2DbContext(DbContextOptions<R2DbContext> options) : base(options) { }

    public DbSet<User> Users => Set<User>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<Versioned> VersionedItems => Set<Versioned>();
    public DbSet<Company> Companies => Set<Company>();
    public DbSet<Dept> Depts => Set<Dept>();
    public DbSet<Employee> Employees => Set<Employee>();
    public DbSet<Folder> Folders => Set<Folder>();
    public DbSet<FileItem> FileItems => Set<FileItem>();
    public DbSet<Post> Posts => Set<Post>();
    public DbSet<Tag> Tags => Set<Tag>();
    public DbSet<Shipment> Shipments => Set<Shipment>();
    public DbSet<GuardedItem> GuardedItems => Set<GuardedItem>();
    public DbSet<NotifyItem> NotifyItems => Set<NotifyItem>();
    public DbSet<SideEffectItem> SideEffectItems => Set<SideEffectItem>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.Entity<User>(b =>
        {
            b.ToTable("Users");
            b.HasMany(x => x.Orders).WithOne(x => x.User).HasForeignKey(x => x.UserId);
        });
        builder.Entity<Order>(b => b.ToTable("Orders"));
        builder.Entity<Versioned>(b =>
        {
            b.ToTable("VersionedItems");
            b.Property(x => x.Version).IsConcurrencyToken();
        });
        builder.Entity<Company>(b =>
        {
            b.ToTable("Companies");
            b.HasMany(x => x.Depts).WithOne().HasForeignKey(x => x.CompanyId);
            b.HasMany(x => x.Folders).WithOne().HasForeignKey(x => x.CompanyId);
        });
        builder.Entity<Dept>(b =>
        {
            b.ToTable("Depts");
            b.HasMany(x => x.Employees).WithOne().HasForeignKey(x => x.DeptId);
        });
        builder.Entity<Employee>(b =>
        {
            b.ToTable("Employees");
            b.HasOne(x => x.Manager).WithMany().HasForeignKey(x => x.ManagerId).OnDelete(DeleteBehavior.Restrict);
        });
        builder.Entity<Folder>(b =>
        {
            b.ToTable("Folders");
            b.HasMany(x => x.Files).WithOne().HasForeignKey(x => x.FolderId);
        });
        builder.Entity<FileItem>(b => b.ToTable("FileItems"));
        builder.Entity<Post>(b =>
        {
            b.ToTable("Posts");
            b.HasMany(x => x.Tags).WithMany();
        });
        builder.Entity<Tag>(b => b.ToTable("Tags"));
        builder.Entity<Shipment>(b =>
        {
            b.ToTable("Shipments");
            b.OwnsMany(x => x.Parcels, p =>
            {
                p.ToTable("Parcels");
                p.WithOwner().HasForeignKey("ShipmentId");
                p.HasKey(x => x.Id);
                p.Property(x => x.Id).ValueGeneratedOnAdd();
            });
        });
        builder.Entity<GuardedItem>(b =>
        {
            b.ToTable("GuardedItems");
            b.Property(x => x.Id).UsePropertyAccessMode(PropertyAccessMode.Property);
            b.Ignore(x => x.RejectNonPositive);
            b.Ignore(x => x.RejectNonPositiveRemaining);
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

/// <summary>Isolated SQLite in-memory host for <see cref="R2DbContext"/>, optionally with a retrying strategy.</summary>
public sealed class R2Host : IDisposable
{
    private static readonly Regex InsertColumns = new(@"INSERT\s+INTO\s+""(?<table>\w+)""\s*\((?<columns>[^)]*)\)", RegexOptions.IgnoreCase);

    private R2Host(SqliteConnection connection, ServiceProvider services, RecordingQueue<int> queue, FaultInjector faults)
    {
        Connection = connection;
        Services = services;
        Queue = queue;
        Faults = faults;
    }

    public SqliteConnection Connection { get; }
    public ServiceProvider Services { get; }
    public RecordingQueue<int> Queue { get; }
    public FaultInjector Faults { get; }

    public static R2Host Create(bool retryingStrategy = false)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingQueue<int>();
        var faults = new FaultInjector();

        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddDbContext<R2DbContext>(options =>
        {
            options.UseSqlite(connection, sqlite =>
            {
                if (retryingStrategy) sqlite.ExecutionStrategy(d => new TestRetryingExecutionStrategy(d));
            });
            options.AddInterceptors(new FaultCommandInterceptor(faults), new FaultCommitInterceptor(faults));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        });
        services.AddOutbox<int>(typeof(R2Host).Assembly);
        services.AddEFCoreOutboxStore<int, R2DbContext>();

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using (var scope = provider.CreateScope())
        {
            scope.ServiceProvider.GetRequiredService<R2DbContext>().Database.EnsureCreated();
        }
        using (var pragma = connection.CreateCommand())
        {
            pragma.CommandText = "PRAGMA foreign_keys = ON";
            pragma.ExecuteNonQuery();
        }
        return new R2Host(connection, provider, queue, faults);
    }

    public (IServiceScope Scope, R2DbContext Db, IOutbox Outbox) CreateScope()
    {
        var scope = Services.CreateScope();
        return (scope, scope.ServiceProvider.GetRequiredService<R2DbContext>(), scope.ServiceProvider.GetRequiredService<IOutbox>());
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

    public List<int> Ints(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var values = new List<int>();
        while (reader.Read()) values.Add(reader.GetInt32(0));
        return values;
    }

    public List<(int Id, string Payload)> OutboxRows()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Payload\" FROM \"OutboxMessages\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, string)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1)));
        return rows;
    }

    public long ForeignKeyViolations()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "PRAGMA foreign_key_check";
        using var reader = command.ExecuteReader();
        long count = 0;
        while (reader.Read()) count++;
        return count;
    }

    /// <summary>INSERT statements into the given tables whose column list contains "Id".</summary>
    public List<string> InsertsWithExplicitId(params string[] tables)
        => Faults.ExecutedCommands
            .SelectMany(sql => InsertColumns.Matches(sql).Cast<Match>())
            .Where(m => tables.Contains(m.Groups["table"].Value) && m.Groups["columns"].Value.Contains("\"Id\""))
            .Select(m => m.Value)
            .ToList();

    public int InsertCount(string table)
        => Faults.ExecutedCommands
            .SelectMany(sql => InsertColumns.Matches(sql).Cast<Match>())
            .Count(m => m.Groups["table"].Value == table);

    public static JsonElement Root(string payload)
    {
        using var doc = JsonDocument.Parse(payload);
        return doc.RootElement.Clone();
    }

    public void Dispose()
    {
        Services.Dispose();
        Connection.Dispose();
    }
}

public static class R2Save
{
    public static Task<int> Invoke(DbContext db, bool async, bool flag)
        => Save.Invoke(db, async, flag);

    public static async Task<Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction> Begin(DbContext db, bool async)
        => async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();

    public static async Task Commit(Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction tx, bool async)
    {
        if (async) await tx.CommitAsync(); else tx.Commit();
    }

    public static async Task Rollback(Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction tx, bool async)
    {
        if (async) await tx.RollbackAsync(); else tx.Rollback();
    }

    public static async Task Savepoint(Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction tx, string name, bool async)
    {
        if (async) await tx.CreateSavepointAsync(name); else tx.CreateSavepoint(name);
    }

    public static async Task RollbackTo(Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction tx, string name, bool async)
    {
        if (async) await tx.RollbackToSavepointAsync(name); else tx.RollbackToSavepoint(name);
    }

    public static async Task Release(Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction tx, string name, bool async)
    {
        if (async) await tx.ReleaseSavepointAsync(name); else tx.ReleaseSavepoint(name);
    }
}
