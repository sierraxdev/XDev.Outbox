using Microsoft.EntityFrameworkCore;

namespace XDev.Outbox.EFCore.Tests.Infrastructure;

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

/// <summary>Entity with a concurrency token, used to prove original values survive a failed save.</summary>
public class Versioned
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Version { get; set; }
}

/// <summary>Event that references a business entity whose key is store-generated (Rule A).</summary>
public class UserCreated
{
    public User? User { get; set; }
}

public class TestDbContext : OutboxDbContext<int>
{
    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options)
    {
    }

    public DbSet<User> Users => Set<User>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<Versioned> VersionedItems => Set<Versioned>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.Entity<User>(b =>
        {
            b.ToTable("Users");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedOnAdd();
            b.HasMany(x => x.Orders).WithOne(x => x.User).HasForeignKey(x => x.UserId);
        });
        builder.Entity<Order>(b =>
        {
            b.ToTable("Orders");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedOnAdd();
        });
        builder.Entity<Versioned>(b =>
        {
            b.ToTable("VersionedItems");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedOnAdd();
            b.Property(x => x.Version).IsConcurrencyToken();
        });

        base.OnModelCreating(builder);
    }
}
