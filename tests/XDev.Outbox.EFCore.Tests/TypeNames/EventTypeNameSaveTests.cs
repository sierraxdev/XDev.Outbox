using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.TypeNames;

/// <summary>The consumer raises the Type column length after base.OnModelCreating.</summary>
public sealed class LongTypeColumnDbContext : OutboxDbContext<int>
{
    public LongTypeColumnDbContext(DbContextOptions<LongTypeColumnDbContext> options) : base(options)
    {
    }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        TypeNameModel.Configure(builder);
        base.OnModelCreating(builder);
        builder.Entity<OutboxMessage<int>>().Property(x => x.Type).HasMaxLength(1000);
    }
}

/// <summary>The consumer removes the Type column length after base.OnModelCreating.</summary>
public sealed class UnboundedTypeColumnDbContext : OutboxDbContext<int>
{
    public UnboundedTypeColumnDbContext(DbContextOptions<UnboundedTypeColumnDbContext> options) : base(options)
    {
    }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        TypeNameModel.Configure(builder);
        base.OnModelCreating(builder);
        builder.Entity<OutboxMessage<int>>().Property(x => x.Type).Metadata.SetMaxLength(null);
    }
}

public static class TypeNameModel
{
    public static void Configure(ModelBuilder builder)
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
    }
}

/// <summary>
/// Saving events whose stored type name is long: the version-less name is stored for every new row; a name still
/// longer than the model's max length for OutboxMessage.Type fails the save with InvalidOperationException before
/// anything is sent to the database, on every provider (SQLite ignores the column length, so only the check stops it).
/// </summary>
public class EventTypeNameSaveTests
{
    // Shortened, this is still far over 200 characters (every argument is qualified with its assembly).
    private static readonly Type TooLongType = typeof(EntityChanged<Dictionary<string, List<KeyValuePair<int, Guid>>>>);

    private readonly ITestOutputHelper _output;

    public EventTypeNameSaveTests(ITestOutputHelper output) => _output = output;

    private static string Expected(Type type) => OutboxTypeName.Get(type);

    public static TheoryData<bool, bool, bool> TransactionAsyncAccept()
    {
        var data = new TheoryData<bool, bool, bool>();
        foreach (var callerTransaction in new[] { false, true })
        foreach (var async in new[] { false, true })
        foreach (var accept in new[] { false, true })
            data.Add(callerTransaction, async, accept);
        return data;
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Save_StoresTheVersionLessNameAndItResolvesToTheEventType(bool callerTransaction, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        if (callerTransaction)
        {
            using var tx = async ? await scope.Db.Database.BeginTransactionAsync() : scope.Db.Database.BeginTransaction();
            await Save.Invoke(scope.Db, async);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }
        else
        {
            await Save.Invoke(scope.Db, async);
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal("XDev.Outbox.EFCore.Tests.Infrastructure.UserCreated, XDev.Outbox.EFCore.Tests", row.Type);
        Assert.Same(typeof(UserCreated), Type.GetType(row.Type, throwOnError: true));
        Assert.Equal(row.Type, Assert.Single(host.Queue.Enqueued).Type);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Save_GenericEventWhoseFullNameExceeds200ButShortNameFits_IsSavedWithTheShortNameAndEnqueued(bool callerTransaction, bool async)
    {
        var eventType = typeof(EntityChanged<User>);
        Assert.True(eventType.AssemblyQualifiedName!.Length > 200, "Precondition: the full name must exceed the 200-character column.");

        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var user = new User { Email = "generic@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new EntityChanged<User> { Entity = user, Change = "created" });

        if (callerTransaction)
        {
            using var tx = async ? await scope.Db.Database.BeginTransactionAsync() : scope.Db.Database.BeginTransaction();
            await Save.Invoke(scope.Db, async);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }
        else
        {
            await Save.Invoke(scope.Db, async);
        }

        var row = Assert.Single(host.OutboxRows());
        _output.WriteLine($"full {eventType.AssemblyQualifiedName.Length}, stored {row.Type.Length}: {row.Type}");
        Assert.Equal(Expected(eventType), row.Type);
        Assert.True(row.Type.Length <= 200);
        Assert.Same(eventType, Type.GetType(row.Type, throwOnError: true));
        using (var payload = System.Text.Json.JsonDocument.Parse(row.Payload))
        {
            Assert.Equal(user.Id, payload.RootElement.GetProperty("entity").GetProperty("id").GetInt32());
        }
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);
        Assert.Equal(row.Type, enqueued.Type);
        Assert.Empty(scope.Outbox.GetEvents());
    }

    [Theory]
    [MemberData(nameof(TransactionAsyncAccept))]
    public async Task Save_WhenTheShortNameStillExceedsTheMaxLength_ThrowsInvalidOperationExceptionBeforeAnythingIsSent(bool callerTransaction, bool async, bool accept)
    {
        var shortName = Expected(TooLongType);
        Assert.True(shortName.Length > 200, "Precondition: the shortened name must still exceed 200 characters.");

        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var user = new User { Email = "long@test" };
        scope.Db.Users.Add(user);
        var tooLong = Activator.CreateInstance(TooLongType)!;
        scope.Outbox.AddEvent(tooLong);

        IDbContextTransaction? tx = callerTransaction
            ? (async ? await scope.Db.Database.BeginTransactionAsync() : scope.Db.Database.BeginTransaction())
            : null;
        try
        {
            host.Faults.ExecutedCommands.Clear();

            var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async, accept));

            var invalid = Assert.IsType<InvalidOperationException>(error);
            _output.WriteLine(invalid.Message);
            Assert.Contains($"'{shortName}'", invalid.Message);
            Assert.DoesNotContain("Version=", invalid.Message);
            Assert.Contains($"{shortName.Length} characters", invalid.Message);
            Assert.Contains("allows at most 200", invalid.Message);
            Assert.Contains(".Property(x => x.Type).HasMaxLength(", invalid.Message);
            Assert.Contains("base.OnModelCreating", invalid.Message);

            // Nothing was sent: no INSERT, no savepoint.
            Assert.Empty(host.Faults.ExecutedCommands);
            Assert.Empty(host.Queue.Enqueued);
            Assert.Equal(new[] { tooLong }, scope.Outbox.GetEvents());
            Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
            Assert.Equal(0, scope.TrackedOutboxMessages);
            Assert.Same(tx, scope.Db.Database.CurrentTransaction);

            if (tx != null)
            {
                // The caller transaction is still usable: drop the event, save and commit.
                scope.Outbox.ClearEvents();
                scope.Outbox.AddEvent(new UserCreated { User = user });
                await Save.Invoke(scope.Db, async, accept);
                if (async) await tx.CommitAsync(); else tx.Commit();
                Assert.Equal(1, host.Count("Users"));
                Assert.Equal(Expected(typeof(UserCreated)), Assert.Single(host.OutboxRows()).Type);
                Assert.Single(host.Queue.Enqueued);
            }
        }
        finally
        {
            tx?.Dispose();
        }

        if (tx == null)
        {
            Assert.Equal(0, host.Count("Users"));
            Assert.Equal(0, host.Count("OutboxMessages"));
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Save_WhenOnlyOneOfSeveralEventsIsTooLong_TheWholeSaveFailsAndEveryEventStaysBuffered(bool callerTransaction, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var user = new User { Email = "mixed@test" };
        scope.Db.Users.Add(user);
        var first = new UserCreated { User = user };
        var tooLong = Activator.CreateInstance(TooLongType)!;
        var last = new EntityChanged<User> { Entity = user };
        scope.Outbox.AddEvent(first, tooLong, last);

        using var tx = callerTransaction ? scope.Db.Database.BeginTransaction() : null;
        host.Faults.ExecutedCommands.Clear();

        var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));

        Assert.IsType<InvalidOperationException>(error);
        Assert.Empty(host.Faults.ExecutedCommands);
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(new object[] { first, tooLong, last }, scope.Outbox.GetEvents());
        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
        Assert.Equal(0, scope.TrackedOutboxMessages);
        tx?.Rollback();
        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
    }

    [Theory]
    [InlineData(typeof(LongTypeColumnDbContext), false)]
    [InlineData(typeof(LongTypeColumnDbContext), true)]
    [InlineData(typeof(UnboundedTypeColumnDbContext), false)]
    [InlineData(typeof(UnboundedTypeColumnDbContext), true)]
    public async Task Save_WhenTheConsumerRaisesOrRemovesTheMaxLength_TheLongNameIsSaved(Type contextType, bool async)
    {
        var task = contextType == typeof(LongTypeColumnDbContext)
            ? SaveTooLongEvent<LongTypeColumnDbContext>(async, expectedMaxLength: 1000)
            : SaveTooLongEvent<UnboundedTypeColumnDbContext>(async, expectedMaxLength: null);
        await task;
    }

    private async Task SaveTooLongEvent<TContext>(bool async, int? expectedMaxLength) where TContext : OutboxDbContext<int>
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingMessageQueue();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddDbContext<TContext>(options =>
        {
            options.UseSqlite(connection);
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        });
        services.AddOutbox(typeof(EventTypeNameSaveTests).Assembly);
        services.AddEFCoreOutboxStore<TContext>();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TContext>();
        db.Database.EnsureCreated();
        Assert.Equal(expectedMaxLength, db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindProperty(nameof(OutboxMessage<int>.Type))!.GetMaxLength());

        db.Add(new User { Email = "override@test" });
        db.Outbox.AddEvent(Activator.CreateInstance(TooLongType)!);
        await Save.Invoke(db, async);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT \"Type\" FROM \"OutboxMessages\"";
        var stored = (string)command.ExecuteScalar()!;
        _output.WriteLine($"{stored.Length}: {stored}");
        Assert.Equal(Expected(TooLongType), stored);
        Assert.True(stored.Length > 200);
        Assert.Same(TooLongType, Type.GetType(stored, throwOnError: true));
        Assert.Single(queue.Enqueued);
    }
}
