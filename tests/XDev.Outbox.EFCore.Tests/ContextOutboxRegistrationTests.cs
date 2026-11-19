using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

public enum ContextRegistration
{
    AddDbContext,
    AddDbContextPool,
    PooledFactory,
    PooledFactoryWithScopedContext,
    Factory,
    FactoryScoped,
}

/// <summary>
/// #7: the event buffer lives on the context (<see cref="OutboxDbContext{TKey, TContext}.Outbox"/>), so every
/// registration kind (scoped, pooled, factory) persists and dispatches the events added to the context that saves,
/// pooled contexts start every lease clean, and the store works without a scoped context.
/// #6: on net8 the options are wrapped in place (lifetime kept, options action run per resolution).
/// </summary>
public class ContextOutboxRegistrationTests
{
    public static TheoryData<ContextRegistration, bool, bool> RegistrationsAsyncAndCallerTransaction()
    {
        var data = new TheoryData<ContextRegistration, bool, bool>();
        foreach (var registration in Enum.GetValues<ContextRegistration>())
        {
            foreach (var async in new[] { false, true })
            {
                data.Add(registration, async, false);
                data.Add(registration, async, true);
            }
        }
        return data;
    }

    public static TheoryData<ContextRegistration, bool> PooledRegistrationsAndAsync()
    {
        var data = new TheoryData<ContextRegistration, bool>();
        foreach (var registration in new[] { ContextRegistration.AddDbContextPool, ContextRegistration.PooledFactory })
        {
            data.Add(registration, false);
            data.Add(registration, true);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(RegistrationsAsyncAndCallerTransaction))]
    public async Task EveryRegistration_EventAddedToTheContext_IsPersistedAndEnqueuedOnce(ContextRegistration registration, bool async, bool callerTransaction)
    {
        using var host = RegistrationHost.Create(registration);
        using var scope = host.Services.CreateScope();
        using var lease = host.Lease(scope);
        var db = lease.Db;

        var user = new User { Email = "a@test" };
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        if (callerTransaction)
        {
            using var transaction = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();
            await Save.Invoke(db, async);
            Assert.Empty(host.Queue.Enqueued);
            if (async) await transaction.CommitAsync(); else transaction.Commit();
        }
        else
        {
            await Save.Invoke(db, async);
        }

        Assert.Equal(1, host.Count("Users"));
        var row = Assert.Single(host.OutboxRows());
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);
        Assert.Equal(user.Id, Payload.User(row.Payload).GetProperty("id").GetInt32());
        Assert.Empty(db.Outbox.GetEvents());
    }

    [Theory]
    [MemberData(nameof(PooledRegistrationsAndAsync))]
    public async Task PooledContext_UnsavedEventOfAnEarlierLease_IsNotSeenOrSavedByTheNextLease(ContextRegistration registration, bool async)
    {
        using var host = RegistrationHost.Create(registration, poolSize: 1);

        Guid instanceId;
        int firstLease;
        using (var scope = host.Services.CreateScope())
        using (var lease = host.Lease(scope))
        {
            instanceId = lease.Db.ContextId.InstanceId;
            firstLease = lease.Db.ContextId.Lease;
            lease.Db.Users.Add(new User { Email = "never-saved@test" });
            lease.Db.Outbox.AddEvent(new UserCreated());
            Assert.Single(lease.Db.Outbox.GetEvents());
        }

        using (var scope = host.Services.CreateScope())
        using (var lease = host.Lease(scope))
        {
            var db = lease.Db;
            Assert.Equal(instanceId, db.ContextId.InstanceId);
            Assert.NotEqual(firstLease, db.ContextId.Lease);
            Assert.Empty(db.Outbox.GetEvents());

            db.Users.Add(new User { Email = "plain@test" });
            await Save.Invoke(db, async);
            Assert.Equal(0, host.Count("OutboxMessages"));
            Assert.Empty(host.Queue.Enqueued);

            var evt = new UserCreated { User = new User { Email = "second@test" } };
            db.Users.Add(evt.User);
            db.Outbox.AddEvent(evt);
            await Save.Invoke(db, async);
        }

        Assert.Equal(2, host.Count("Users"));
        var row = Assert.Single(host.OutboxRows());
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);
        Assert.Equal("second@test", Payload.User(row.Payload).GetProperty("email").GetString());
    }

    [Theory]
    [MemberData(nameof(PooledRegistrationsAndAsync))]
    public async Task PooledContext_CallerTransactionLeftOpenAfterAcceptFalseSave_NothingLeaksIntoTheNextLease(ContextRegistration registration, bool async)
    {
        using var host = RegistrationHost.Create(registration, poolSize: 1);

        Guid instanceId;
        using (var scope = host.Services.CreateScope())
        using (var lease = host.Lease(scope))
        {
            var db = lease.Db;
            instanceId = db.ContextId.InstanceId;
            var transaction = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();
            var user = new User { Email = "rolled-back@test" };
            db.Users.Add(user);
            db.Outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
            // The transaction is neither committed nor rolled back: the lease ends with it open.
        }

        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));

        using (var scope = host.Services.CreateScope())
        using (var lease = host.Lease(scope))
        {
            var db = lease.Db;
            Assert.Equal(instanceId, db.ContextId.InstanceId);
            Assert.Empty(db.Outbox.GetEvents());
            Assert.Empty(db.ChangeTracker.Entries());

            // A plain save inside a new caller transaction: no stale pending message is re-persisted or dispatched.
            using (var transaction = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
            {
                db.Users.Add(new User { Email = "next@test" });
                await Save.Invoke(db, async);
                if (async) await transaction.CommitAsync(); else transaction.Commit();
            }

            // And an owned save without events.
            db.Users.Add(new User { Email = "owned@test" });
            await Save.Invoke(db, async);
        }

        Assert.Equal(2, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(ContextRegistration.AddDbContext)]
    [InlineData(ContextRegistration.AddDbContextPool)]
    [InlineData(ContextRegistration.PooledFactory)]
    public void Outbox_IsNotRegisteredInDependencyInjection_AndIsOnePerContext(ContextRegistration registration)
    {
        using var host = RegistrationHost.Create(registration);
        using var scope = host.Services.CreateScope();

        Assert.Null(scope.ServiceProvider.GetService<IOutbox>());

        using var lease = host.Lease(scope);
        var outbox = lease.Db.Outbox;
        Assert.NotNull(outbox);
        Assert.Same(outbox, lease.Db.Outbox);
        var applicationServices = lease.Db.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.ApplicationServiceProvider;
        Assert.Null(applicationServices?.GetService(typeof(IOutbox)));

        using var otherScope = host.Services.CreateScope();
        using var other = host.Lease(otherScope);
        Assert.NotSame(lease.Db, other.Db);
        Assert.NotSame(outbox, other.Db.Outbox);
    }

    [Fact]
    public void Model_DoesNotMapTheEventBuffer()
    {
        using var host = RegistrationHost.Create(ContextRegistration.AddDbContext);
        using var scope = host.Services.CreateScope();
        using var lease = host.Lease(scope);

        var mapped = lease.Db.Model.GetEntityTypes().Select(x => x.ClrType).OrderBy(x => x.Name).ToList();
        Assert.Equal(new[] { typeof(Order), typeof(OutboxMessage<int>), typeof(User), typeof(Versioned) }.OrderBy(x => x.Name), mapped);
        Assert.DoesNotContain(
            lease.Db.Model.FindEntityType(typeof(OutboxMessage<int>))!.GetProperties(),
            x => x.Name.Contains("Outbox", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(ContextRegistration.AddDbContext)]
    [InlineData(ContextRegistration.AddDbContextPool)]
    [InlineData(ContextRegistration.PooledFactory)]
    [InlineData(ContextRegistration.Factory)]
    [InlineData(ContextRegistration.FactoryScoped)]
    public void AddEFCoreOutboxStore_KeepsTheLifetimeOfTheRegisteredOptions(ContextRegistration registration)
    {
        var services = new ServiceCollection();
        RegistrationHost.RegisterContext(services, registration, options => options.UseSqlite("Data Source=:memory:"), poolSize: 1024);
        var before = services.Last(x => x.ServiceType == typeof(DbContextOptions<TestDbContext>)).Lifetime;

        services.AddOutbox(typeof(ContextOutboxRegistrationTests).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();

        var after = services.Last(x => x.ServiceType == typeof(DbContextOptions<TestDbContext>)).Lifetime;
        Assert.Equal(before, after);
        Assert.Equal(1, services.Count(x => x.ServiceType == typeof(DbContextOptions<TestDbContext>)));
    }

    [Fact]
    public void ScopedOptionsAction_RunsForEveryScope_WithThatScopesServices()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(new RecordingMessageQueue());
        services.AddScoped<Tenant>();
        services.AddDbContext<TestDbContext>((sp, options) =>
        {
            options.UseSqlite(connection);
            options.AddInterceptors(new TenantMarker(sp.GetRequiredService<Tenant>()));
            options.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
        });
        services.AddOutbox(typeof(ContextOutboxRegistrationTests).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var seen = new List<Tenant>();
        for (var i = 0; i < 2; i++)
        {
            using var scope = provider.CreateScope();
            var tenant = scope.ServiceProvider.GetRequiredService<Tenant>();
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var interceptors = Interceptors(db);
            Assert.Same(tenant, interceptors.OfType<TenantMarker>().Single().Tenant);
            Assert.Single(interceptors.OfType<OutboxDispatchTransactionInterceptor<int>>());
            seen.Add(tenant);
        }
        Assert.NotSame(seen[0], seen[1]);
    }

    [Theory]
    [InlineData(ContextRegistration.AddDbContext)]
    [InlineData(ContextRegistration.AddDbContextPool)]
    [InlineData(ContextRegistration.PooledFactory)]
    public async Task AddEFCoreOutboxStoreTwice_AttachesTheInterceptorOnce(ContextRegistration registration)
    {
        using var host = RegistrationHost.Create(registration, configure: services => services.AddEFCoreOutboxStore<TestDbContext>());
        using var scope = host.Services.CreateScope();
        using var lease = host.Lease(scope);
        var db = lease.Db;

        Assert.Single(Interceptors(db).OfType<OutboxDispatchTransactionInterceptor<int>>());

        using (var transaction = await db.Database.BeginTransactionAsync())
        {
            db.Users.Add(new User { Email = "once@test" });
            db.Outbox.AddEvent(new UserCreated());
            await db.SaveChangesAsync();
            await transaction.CommitAsync();
        }
        Assert.Single(host.Queue.Enqueued);
    }

    [Fact]
    public void KeyedOptionsRegisteredAfterTheContext_AreIgnored()
    {
        using var host = RegistrationHost.Create(
            ContextRegistration.AddDbContext,
            beforeOutbox: services => services.AddKeyedSingleton("other", (sp, key) => new DbContextOptionsBuilder<TestDbContext>().Options));
        using var scope = host.Services.CreateScope();
        using var lease = host.Lease(scope);

        Assert.Single(Interceptors(lease.Db).OfType<OutboxDispatchTransactionInterceptor<int>>());
    }

#if !NET9_0_OR_GREATER
    [Fact]
    public void Net8_AddEFCoreOutboxStoreWithoutARegisteredContext_Throws()
    {
        var services = new ServiceCollection();
        services.AddOutbox(typeof(ContextOutboxRegistrationTests).Assembly);

        var ex = Assert.Throws<InvalidOperationException>(() => services.AddEFCoreOutboxStore<TestDbContext>());
        Assert.Contains("AddPooledDbContextFactory", ex.Message);
    }
#endif

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Store_WithAddPooledDbContextFactoryOnly_ResolvesWorksAndReturnsItsContextToThePool(bool asyncDispose)
    {
        // EF 8/9 register no scoped context for AddPooledDbContextFactory (the store falls back to the factory);
        // EF 10+ also register a scoped context leased from the pool. Either way the store works with scope
        // validation on and its context goes back to the pool with the scope.
        using var host = RegistrationHost.Create(ContextRegistration.PooledFactory, poolSize: 1);
        var id = host.SeedOutboxRow();

        if (asyncDispose)
        {
            await using var scope = host.Services.CreateAsyncScope();
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore<int>>();
            await store.RemoveAsync(new OutboxMessage<int> { Id = id, Type = "t", Payload = new object() });
        }
        else
        {
            using var scope = host.Services.CreateScope();
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore<int>>();
            await store.RemoveAsync(new OutboxMessage<int> { Id = id, Type = "t", Payload = new object() });
        }

        Assert.Equal(0, host.Count("OutboxMessages"));

        // poolSize 1: the next context is the store's instance, second lease.
        using var next = host.Services.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext();
        Assert.Equal(2, next.ContextId.Lease);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Store_WithoutAScopedContext_CreatesOneFromTheFactoryAndDisposesIt(bool asyncDispose)
    {
        using var host = RegistrationHost.Create(ContextRegistration.PooledFactory, poolSize: 1);
        var id = host.SeedOutboxRow();
        var factory = host.Services.GetRequiredService<IDbContextFactory<TestDbContext>>();
        using var factoryOnly = new ServiceCollection().AddSingleton(factory).BuildServiceProvider();
        Assert.Null(factoryOnly.GetService<TestDbContext>());

        var store = new EntityFrameworkOutboxStore<int, TestDbContext>(factoryOnly, TimeProvider.System);
        await store.UpdateAsync(new OutboxMessage<int> { Id = id, Type = "updated", Payload = new { }, Attemps = 3 });
        if (asyncDispose) await store.DisposeAsync(); else store.Dispose();

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal("updated", row.Type);

        // poolSize 1: the store's context was returned to the pool; the next context is that instance, second lease.
        using var next = factory.CreateDbContext();
        Assert.Equal(2, next.ContextId.Lease);
    }

    [Fact]
    public void Store_WithAScopedContext_DoesNotDisposeIt()
    {
        using var host = RegistrationHost.Create(ContextRegistration.AddDbContext);
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();

        var store = new EntityFrameworkOutboxStore<int, TestDbContext>(scope.ServiceProvider, TimeProvider.System);
        store.Dispose();

        Assert.Equal(0, db.Users.Count());
    }

    [Fact]
    public void Store_WithNeitherContextNorFactory_ThrowsNamingTheRegistrations()
    {
        var provider = new ServiceCollection().BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() => new EntityFrameworkOutboxStore<int, TestDbContext>(provider, TimeProvider.System));
        Assert.Contains("AddDbContext", ex.Message);
        Assert.Contains("AddPooledDbContextFactory", ex.Message);
    }

    private static IReadOnlyList<IInterceptor> Interceptors(DbContext db)
        => db.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.Interceptors?.ToList() ?? new List<IInterceptor>();

    public sealed class Tenant
    {
    }

    private sealed class TenantMarker : IInterceptor
    {
        public TenantMarker(Tenant tenant) => Tenant = tenant;

        public Tenant Tenant { get; }
    }
}
