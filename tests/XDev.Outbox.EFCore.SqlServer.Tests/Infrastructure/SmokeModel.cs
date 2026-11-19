using Microsoft.EntityFrameworkCore;

namespace XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

public class User
{
    public int Id { get; set; }
    public string Email { get; set; } = "";
    public List<Order> Orders { get; set; } = new();
}

public class Order
{
    public int Id { get; set; }
    public int UserId { get; set; }
    public User? User { get; set; }
    public string Description { get; set; } = "";
}

/// <summary>References a principal (and its dependents) with identity keys: Rule A forces the payload UPDATE.</summary>
public class UserCreated
{
    public User? User { get; set; }
    public string? Text { get; set; }
}

/// <summary>References a dependent: its payload carries the order's identity key and the propagated FK.</summary>
public class OrderPlaced
{
    public Order? Order { get; set; }
}

/// <summary>No entity reference: its payload is final at INSERT time, so no UPDATE runs.</summary>
public class TextEvent
{
    public string? Text { get; set; }
}

/// <summary>Generic event whose assembly-qualified name is far longer than the 200-character Type column.</summary>
public class LongNamedEvent<T>
{
    public User? User { get; set; }
}

/// <summary>Dispatched through the real hosted service.</summary>
public class ProbeEvent
{
    public string? Text { get; set; }
    public int Number { get; set; }
}

public abstract class SmokeContextBase<TKey> : OutboxDbContext<TKey> where TKey : struct
{
    protected SmokeContextBase(DbContextOptions options) : base(options)
    {
    }

    public DbSet<User> Users => Set<User>();
    public DbSet<Order> Orders => Set<Order>();

    /// <summary>Model default schema (null = none).</summary>
    protected virtual string? DefaultSchema => null;

    /// <summary>Called after the library configured the outbox entity.</summary>
    protected virtual void AfterOutboxConfigured(ModelBuilder builder)
    {
    }

    protected override void OnModelCreating(ModelBuilder builder)
    {
        if (DefaultSchema != null) builder.HasDefaultSchema(DefaultSchema);

        builder.Entity<User>(b =>
        {
            b.ToTable("Users");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).UseIdentityColumn();
            b.Property(x => x.Email).HasMaxLength(400);
            b.HasMany(x => x.Orders).WithOne(x => x.User).HasForeignKey(x => x.UserId);
        });
        builder.Entity<Order>(b =>
        {
            b.ToTable("Orders");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).UseIdentityColumn();
        });

        base.OnModelCreating(builder);
        AfterOutboxConfigured(builder);
    }
}

/// <summary>Default shape: int outbox key, dbo.OutboxMessages.</summary>
public sealed class SmokeDbContext : SmokeContextBase<int>
{
    public SmokeDbContext(DbContextOptions<SmokeDbContext> options) : base(options)
    {
    }
}

/// <summary>Registered with <c>AddEFCoreOutboxStore(o =&gt; o.TableName = "Outbox{Msgs}")</c>.</summary>
public sealed class BracedTableDbContext : SmokeContextBase<int>
{
    public const string TableName = "Outbox{Msgs}";

    public BracedTableDbContext(DbContextOptions<BracedTableDbContext> options) : base(options)
    {
    }
}

/// <summary>Registered with <c>AddEFCoreOutboxStore(o =&gt; o.TableName = "SmokeOutbox")</c>.</summary>
public sealed class CustomTableDbContext : SmokeContextBase<int>
{
    public const string TableName = "SmokeOutbox";

    public CustomTableDbContext(DbContextOptions<CustomTableDbContext> options) : base(options)
    {
    }
}

/// <summary>The consumer moves only the outbox table to schema "outbox" with ToTable(name, schema).</summary>
public sealed class OutboxSchemaDbContext : SmokeContextBase<int>
{
    public const string Schema = "outbox";

    public OutboxSchemaDbContext(DbContextOptions<OutboxSchemaDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable("OutboxMessages", Schema);
}

/// <summary>The outbox table's schema and name both need delimiting (space, closing bracket, brace).</summary>
public sealed class QuotedOutboxTableDbContext : SmokeContextBase<int>
{
    public const string Schema = "out box]";
    public const string Table = "Outbox] {Msgs}";

    public QuotedOutboxTableDbContext(DbContextOptions<QuotedOutboxTableDbContext> options) : base(options)
    {
    }

    protected override void AfterOutboxConfigured(ModelBuilder builder)
        => builder.Entity<OutboxMessage<int>>().ToTable(Table, Schema);
}

/// <summary>The whole model lives in schema "app" (HasDefaultSchema).</summary>
public sealed class DefaultSchemaDbContext : SmokeContextBase<int>
{
    public const string Schema = "app";

    public DefaultSchemaDbContext(DbContextOptions<DefaultSchemaDbContext> options) : base(options)
    {
    }

    protected override string? DefaultSchema => Schema;
}

/// <summary>Guid outbox key.</summary>
public sealed class GuidDbContext : SmokeContextBase<Guid>
{
    public GuidDbContext(DbContextOptions<GuidDbContext> options) : base(options)
    {
    }
}
