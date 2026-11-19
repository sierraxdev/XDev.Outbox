using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>
/// #6 (net8 options wrapping) adversarially, on every TFM: options actions run per resolution with the real
/// container (never at registration), consumer singletons resolved in the options action are the app's instances,
/// pooling/factories with scope validation build and save, and registration order.
/// </summary>
public class OptionsRegistrationAdversarialTests
{
    public sealed class AppSingletonInterceptor : DbCommandInterceptor
    {
    }

    public sealed class ScopedTenant
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    public sealed class TenantTag : IInterceptor
    {
        public TenantTag(ScopedTenant tenant) => Tenant = tenant;
        public ScopedTenant Tenant { get; }
    }

    private static SqliteConnection OpenWithSchema()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var schemaOptions = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(connection)
            .UseApplicationServiceProvider(new ServiceCollection().AddOptions().BuildServiceProvider())
            .ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning))
            .Options;
        using var schema = new TestDbContext(schemaOptions);
        schema.Database.EnsureCreated();
        return connection;
    }

    public static TheoryData<ContextRegistration> AllRegistrations()
    {
        var data = new TheoryData<ContextRegistration>();
        foreach (var r in Enum.GetValues<ContextRegistration>()) data.Add(r);
        return data;
    }

    [Theory]
    [MemberData(nameof(AllRegistrations))]
    public async Task OptionsActionWithAppSingleton_NotRunAtRegistration_SingletonIsTheAppInstance_SavesWithScopeValidation(ContextRegistration registration)
    {
        using var connection = OpenWithSchema();
        var queue = new RecordingMessageQueue();
        var appInterceptor = new AppSingletonInterceptor();
        var optionsActionRuns = 0;
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddSingleton(appInterceptor);
        services.AddLogging();

        Action<IServiceProvider, DbContextOptionsBuilder> action = (sp, o) =>
        {
            Interlocked.Increment(ref optionsActionRuns);
            o.UseSqlite(connection);
            o.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
            o.AddInterceptors(sp.GetRequiredService<AppSingletonInterceptor>());
        };
        switch (registration)
        {
            case ContextRegistration.AddDbContext: services.AddDbContext<TestDbContext>(action); break;
            case ContextRegistration.AddDbContextPool: services.AddDbContextPool<TestDbContext>(action, 2); break;
            case ContextRegistration.PooledFactory: services.AddPooledDbContextFactory<TestDbContext>(action, 2); break;
            case ContextRegistration.PooledFactoryWithScopedContext:
                services.AddPooledDbContextFactory<TestDbContext>(action, 2);
                services.AddScoped(sp => sp.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext());
                break;
            case ContextRegistration.Factory: services.AddDbContextFactory<TestDbContext>(action); break;
            case ContextRegistration.FactoryScoped: services.AddDbContextFactory<TestDbContext>(action, ServiceLifetime.Scoped); break;
        }
        services.AddOutbox(typeof(OptionsRegistrationAdversarialTests).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        Assert.Equal(0, optionsActionRuns); // #6: no temporary container runs the action at registration.

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        for (var i = 0; i < 2; i++)
        {
            await using var scope = provider.CreateAsyncScope();
            var factoryCreated = registration is ContextRegistration.PooledFactory or ContextRegistration.Factory or ContextRegistration.FactoryScoped;
            var db = factoryCreated
                ? scope.ServiceProvider.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext()
                : scope.ServiceProvider.GetRequiredService<TestDbContext>();
            try
            {
                var interceptors = ContextIsolationAdversarialTests.Interceptors(db);
                Assert.Same(appInterceptor, Assert.Single(interceptors.OfType<AppSingletonInterceptor>()));
                Assert.Same(provider.GetRequiredService<OutboxDispatchTransactionInterceptor<int>>(), Assert.Single(interceptors.OfType<OutboxDispatchTransactionInterceptor<int>>()));

                using (var tx = await db.Database.BeginTransactionAsync())
                {
                    var u = new User { Email = $"{i}@test" };
                    db.Users.Add(u);
                    db.Outbox.AddEvent(new UserCreated { User = u });
                    await db.SaveChangesAsync();
                    await tx.CommitAsync();
                }
            }
            finally
            {
                if (factoryCreated) await db.DisposeAsync();
            }
        }
        Assert.Equal(2, queue.Enqueued.Count);
        Assert.True(optionsActionRuns >= 1);
        if (registration is ContextRegistration.AddDbContext or ContextRegistration.FactoryScoped)
        {
            Assert.Equal(2, optionsActionRuns); // once per scope
        }
    }

    [Theory]
    [InlineData(ContextRegistration.AddDbContext)]
    [InlineData(ContextRegistration.FactoryScoped)]
    public void ScopedOptions_OptionsActionResolvingAScopedService_SeesEachScopesInstance(ContextRegistration registration)
    {
        using var connection = OpenWithSchema();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(new RecordingMessageQueue());
        services.AddScoped<ScopedTenant>();
        Action<IServiceProvider, DbContextOptionsBuilder> action = (sp, o) =>
        {
            o.UseSqlite(connection);
            o.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
            o.AddInterceptors(new TenantTag(sp.GetRequiredService<ScopedTenant>()));
        };
        if (registration == ContextRegistration.AddDbContext) services.AddDbContext<TestDbContext>(action);
        else services.AddDbContextFactory<TestDbContext>(action, ServiceLifetime.Scoped);
        services.AddOutbox(typeof(OptionsRegistrationAdversarialTests).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var seen = new List<Guid>();
        for (var i = 0; i < 2; i++)
        {
            using var scope = provider.CreateScope();
            var tenant = scope.ServiceProvider.GetRequiredService<ScopedTenant>();
            using var db = registration == ContextRegistration.FactoryScoped
                ? scope.ServiceProvider.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext()
                : scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var tag = Assert.Single(ContextIsolationAdversarialTests.Interceptors(db).OfType<TenantTag>());
            Assert.Same(tenant, tag.Tenant);
            Assert.Single(ContextIsolationAdversarialTests.Interceptors(db).OfType<OutboxDispatchTransactionInterceptor<int>>());
            seen.Add(tenant.Id);
        }
        Assert.NotEqual(seen[0], seen[1]);
    }

    [Fact]
    public async Task AddEFCoreOutboxStoreBeforeAddDbContext_Net8ThrowsClearError_Net9PlusWorks()
    {
        using var connection = OpenWithSchema();
        var queue = new RecordingMessageQueue();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddOutbox(typeof(OptionsRegistrationAdversarialTests).Assembly);

#if NET9_0_OR_GREATER
        services.AddEFCoreOutboxStore<TestDbContext>();
        services.AddDbContext<TestDbContext>(o => o.UseSqlite(connection).ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning)));
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.Single(ContextIsolationAdversarialTests.Interceptors(db).OfType<OutboxDispatchTransactionInterceptor<int>>());
        using (var tx = await db.Database.BeginTransactionAsync())
        {
            db.Users.Add(new User { Email = "order@test" });
            db.Outbox.AddEvent(new UserCreated());
            await db.SaveChangesAsync();
            await tx.CommitAsync();
        }
        Assert.Single(queue.Enqueued);
#else
        var ex = Assert.Throws<InvalidOperationException>(() => services.AddEFCoreOutboxStore<TestDbContext>());
        Assert.Contains(nameof(TestDbContext), ex.Message);
        Assert.Contains("before calling AddEFCoreOutboxStore", ex.Message);
        await Task.CompletedTask;
#endif
    }

    /// <summary>
    /// AddDbContext registered twice (EF uses TryAdd for the options, so the first action wins) and the outbox added in
    /// between: one interceptor, and the events still dispatch.
    /// </summary>
    [Fact]
    public async Task AddDbContextCalledAgainAfterAddEFCoreOutboxStore_InterceptorStillAttachedOnce()
    {
        using var connection = OpenWithSchema();
        var queue = new RecordingMessageQueue();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddDbContext<TestDbContext>(o => o.UseSqlite(connection).ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning)));
        services.AddOutbox(typeof(OptionsRegistrationAdversarialTests).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        services.AddDbContext<TestDbContext>(o => o.UseSqlite(connection).ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning)));
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.Single(ContextIsolationAdversarialTests.Interceptors(db).OfType<OutboxDispatchTransactionInterceptor<int>>());
        using (var tx = await db.Database.BeginTransactionAsync())
        {
            db.Users.Add(new User { Email = "twice@test" });
            db.Outbox.AddEvent(new UserCreated());
            await db.SaveChangesAsync();
            await tx.CommitAsync();
        }
        Assert.Single(queue.Enqueued);
    }
}
