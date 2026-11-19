using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>
/// Adversarial isolation: events never cross contexts (two contexts in one scope), leases (stale references), or
/// registrations; the processor/subscriber pattern persists follow-up events atomically with the delete; extra
/// interceptor instances do not double-dispatch; plain DbContexts with the interceptor are ignored.
/// </summary>
public class ContextIsolationAdversarialTests
{
    private static int EfMajor => typeof(DbContext).Assembly.GetName().Version!.Major;

    // ---------------- #4 two contexts in the same scope ----------------

    public static TheoryData<ContextRegistration, bool> TwoContextRegistrations()
    {
        var data = new TheoryData<ContextRegistration, bool>();
        foreach (var r in new[] { ContextRegistration.AddDbContext, ContextRegistration.PooledFactoryWithScopedContext, ContextRegistration.FactoryScoped, ContextRegistration.Factory })
        {
            data.Add(r, false);
            data.Add(r, true);
        }
        return data;
    }

    private static TestDbContext SecondContext(PoolHost host, IServiceProvider scope)
    {
        switch (host.Registration)
        {
            case ContextRegistration.AddDbContext:
                // A second instance built from the same (scoped) options, as a hand-built context in the same scope.
                return ActivatorUtilities.CreateInstance<TestDbContext>(scope);
            default:
                return scope.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext();
        }
    }

    [Theory]
    [MemberData(nameof(TwoContextRegistrations))]
    public async Task TwoContextsInOneScope_EventAddedToOne_IsSavedOnlyByThatOne(ContextRegistration registration, bool async)
    {
        using var host = PoolHost.Create(registration, poolSize: 4);
        var lease = host.Lease();
        var other = SecondContext(host, lease.Services);
        try
        {
            var a = lease.Db;
            Assert.NotSame(a, other);
            Assert.NotSame(a.Outbox, other.Outbox);

            var userA = new User { Email = "a@test" };
            a.Users.Add(userA);
            var evtA = new UserCreated { User = userA };
            a.Outbox.AddEvent(evtA);

            // Saving the other context writes none of A's events, owned and inside a caller transaction.
            other.Users.Add(new User { Email = "b-owned@test" });
            await Save.Invoke(other, async);
            using (var tx = await PoolReuseAdversarialTests.Begin(other, async))
            {
                other.Users.Add(new User { Email = "b-tx@test" });
                await Save.Invoke(other, async);
                if (async) await tx.CommitAsync(); else tx.Commit();
            }
            Assert.Equal(0, host.Count("OutboxMessages"));
            Assert.Empty(host.Queue.Enqueued);
            Assert.Empty(other.Outbox.GetEvents());
            Assert.Same(evtA, Assert.Single(a.Outbox.GetEvents()));

            // B's own event is saved by B only; A's save then writes only A's.
            var userB = new User { Email = "b-evt@test" };
            other.Users.Add(userB);
            other.Outbox.AddEvent(new UserCreated { User = userB });
            await Save.Invoke(other, async);
            Assert.Same(evtA, Assert.Single(a.Outbox.GetEvents()));

            await Save.Invoke(a, async);

            var rows = host.OutboxRows();
            Assert.Equal(2, rows.Count);
            Assert.Equal("b-evt@test", Payload.User(rows[0].Payload).GetProperty("email").GetString());
            Assert.Equal("a@test", Payload.User(rows[1].Payload).GetProperty("email").GetString());
            Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(e => e.Id));
        }
        finally
        {
            if (async) await other.DisposeAsync(); else other.Dispose();
            await lease.End(async);
        }
    }

    // ---------------- #5 subscriber pattern ----------------

    public static TheoryData<ContextRegistration> SubscriberRegistrations() => new()
    {
        ContextRegistration.AddDbContext,
        ContextRegistration.AddDbContextPool,
        ContextRegistration.PooledFactoryWithScopedContext,
        ContextRegistration.Factory,
        ContextRegistration.FactoryScoped,
    };

    /// <summary>
    /// Processor-like scope: the store and the subscriber share the scope's context, so a follow-up event added by the
    /// subscriber is inserted in the same transaction as the processed message's delete.
    /// </summary>
    [Theory]
    [MemberData(nameof(SubscriberRegistrations))]
    public async Task ProcessorScope_SubscriberAddsFollowUpEvent_PersistedAtomicallyWithTheDelete(ContextRegistration registration)
    {
        using var host = PoolHost.Create(registration, poolSize: 1);
        var processed = host.SeedOutboxRow();

        // Warm-up lease so pooled registrations reuse the instance.
        var warm = host.Lease();
        await warm.End(false);

        // 1st attempt: the follow-up INSERT fails → neither the delete nor the follow-up row is committed.
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore<int>>();
            var subscriberDb = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            subscriberDb.Outbox.AddEvent(new UserCreated { User = new User { Email = "follow-up@test" } });
            host.Faults.FailCommands(SqlMatch.IsOutboxInsert);
            await Assert.ThrowsAnyAsync<Exception>(() => store.RemoveAsync(new OutboxMessage<int> { Id = processed, Type = "t", Payload = new object() }));
            host.Faults.Clear();
        }
        Assert.Equal(new[] { processed }, host.OutboxRows().Select(r => r.Id));
        Assert.Empty(host.Queue.Enqueued);

        // 2nd attempt (new processor scope): delete + follow-up in one transaction, follow-up dispatched once.
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore<int>>();
            var subscriberDb = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            Assert.Empty(subscriberDb.Outbox.GetEvents());
            subscriberDb.Outbox.AddEvent(new UserCreated { User = new User { Email = "follow-up@test" } });
            await store.RemoveAsync(new OutboxMessage<int> { Id = processed, Type = "t", Payload = new object() });
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.NotEqual(processed, row.Id);
        Assert.Equal("follow-up@test", Payload.User(row.Payload).GetProperty("email").GetString());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
    }

    /// <summary>
    /// Documents AddPooledDbContextFactory alone. EF 8/9 register no scoped context: the store uses its own context from
    /// the factory, so a subscriber's context (also from the factory) is a different one and its follow-up event is not
    /// saved by the store's RemoveAsync (lost unless the subscriber saves its own context, which is then not atomic with
    /// the delete). EF 10+ register a scoped context and the pattern is atomic.
    /// </summary>
    [Fact]
    public async Task ProcessorScope_PooledFactoryOnly_DocumentsWhetherTheSubscriberSharesTheStoresContext()
    {
        using var host = PoolHost.Create(ContextRegistration.PooledFactory, poolSize: 4);
        var processed = host.SeedOutboxRow();

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore<int>>();
            var scoped = scope.ServiceProvider.GetService<TestDbContext>();
            if (EfMajor >= 10)
            {
                Assert.NotNull(scoped);
                scoped!.Outbox.AddEvent(new UserCreated { User = new User { Email = "follow-up@test" } });
                await store.RemoveAsync(new OutboxMessage<int> { Id = processed, Type = "t", Payload = new object() });
            }
            else
            {
                Assert.Null(scoped);
                await using var subscriberDb = await scope.ServiceProvider.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContextAsync();
                subscriberDb.Outbox.AddEvent(new UserCreated { User = new User { Email = "follow-up@test" } });
                await store.RemoveAsync(new OutboxMessage<int> { Id = processed, Type = "t", Payload = new object() });
                // The store's save did not see the subscriber's buffer.
                Assert.Single(subscriberDb.Outbox.GetEvents());
            }
        }

        if (EfMajor >= 10)
        {
            var row = Assert.Single(host.OutboxRows());
            Assert.NotEqual(processed, row.Id);
            Assert.Single(host.Queue.Enqueued);
        }
        else
        {
            Assert.Empty(host.OutboxRows());
            Assert.Empty(host.Queue.Enqueued);
        }
    }

    // ---------------- #6 stale references ----------------

    [Theory]
    [InlineData(ContextRegistration.AddDbContextPool, false)]
    [InlineData(ContextRegistration.AddDbContextPool, true)]
    [InlineData(ContextRegistration.PooledFactory, false)]
    [InlineData(ContextRegistration.PooledFactory, true)]
    public async Task StalePooledReference_EventAddedAfterTheLeaseEnded_IsNotSavedByTheNextLease(ContextRegistration registration, bool asyncDispose)
    {
        using var host = PoolHost.Create(registration, poolSize: 1);
        var first = host.Lease();
        var stale = first.Db;
        await first.End(asyncDispose);

        // Use after the lease ended (misuse): EF operations throw ObjectDisposedException; the buffer does not check.
        Assert.Throws<ObjectDisposedException>(() => stale.Users.Add(new User { Email = "stale@test" }));
        stale.Outbox.AddEvent(new UserCreated { User = new User { Email = "stale@test" } });

        var next = host.Lease();
        try
        {
            Assert.Same(stale, next.Db);
            Assert.Empty(next.Db.Outbox.GetEvents());
            next.Db.Users.Add(new User { Email = "next@test" });
            await Save.Invoke(next.Db, asyncDispose);
        }
        finally
        {
            await next.End(asyncDispose);
        }

        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// Documents the limit of the lease check: a stale reference used while the same instance is leased again is
    /// indistinguishable from the current lease (EF has the same limitation for every operation, see the Users.Add
    /// below), so an event added through it lands in the current lease's buffer.
    /// </summary>
    [Fact]
    public async Task StalePooledReference_UsedWhileTheInstanceIsLeasedAgain_EventLandsInTheCurrentLease_Documented()
    {
        using var host = PoolHost.Create(ContextRegistration.AddDbContextPool, poolSize: 1);
        var first = host.Lease();
        var stale = first.Db;
        await first.End(false);

        var next = host.Lease();
        try
        {
            Assert.Same(stale, next.Db);
            stale.Outbox.AddEvent(new UserCreated { User = new User { Email = "stale@test" } });
            stale.Users.Add(new User { Email = "stale-entity@test" }); // EF accepts it too: same object.
            Assert.Single(next.Db.Outbox.GetEvents());
            await next.Db.SaveChangesAsync();
        }
        finally
        {
            await next.End(false);
        }

        Assert.Single(host.OutboxRows());
    }

    [Fact]
    public async Task DisposedNonPooledContext_AddEventDoesNotThrow_SaveThrowsObjectDisposed_Documented()
    {
        using var host = PoolHost.Create(ContextRegistration.AddDbContext);
        var lease = host.Lease();
        var db = lease.Db;
        await lease.End(false);

        db.Outbox.AddEvent(new UserCreated());
        Assert.Single(db.Outbox.GetEvents());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => db.SaveChangesAsync());
        Assert.Throws<ObjectDisposedException>(() => db.SaveChanges());
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    // ---------------- #7 extra interceptor instances ----------------

    public static TheoryData<ContextRegistration, bool, bool> ExtraInterceptorCases()
    {
        var data = new TheoryData<ContextRegistration, bool, bool>();
        foreach (var r in new[] { ContextRegistration.AddDbContext, ContextRegistration.AddDbContextPool, ContextRegistration.PooledFactory })
        foreach (var sameInstance in new[] { false, true })
        foreach (var async in new[] { false, true })
            data.Add(r, sameInstance, async);
        return data;
    }

    [Theory]
    [MemberData(nameof(ExtraInterceptorCases))]
    public async Task ConsumerAlsoAttachesADispatchInterceptor_NoDoubleDispatch(ContextRegistration registration, bool sameInstance, bool async)
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingMessageQueue();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddSingleton<OutboxDispatchTransactionInterceptor<int>>();
        Action<IServiceProvider, DbContextOptionsBuilder> configure = (sp, o) =>
        {
            o.UseSqlite(connection);
            o.ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning));
            o.AddInterceptors(sameInstance
                ? sp.GetRequiredService<OutboxDispatchTransactionInterceptor<int>>()
                : new OutboxDispatchTransactionInterceptor<int>(sp.GetRequiredService<IMessageQueue<int>>()));
        };
        switch (registration)
        {
            case ContextRegistration.AddDbContext: services.AddDbContext<TestDbContext>(configure); break;
            case ContextRegistration.AddDbContextPool: services.AddDbContextPool<TestDbContext>(configure, 1); break;
            default: services.AddPooledDbContextFactory<TestDbContext>(configure, 1); break;
        }
        services.AddOutbox(typeof(ContextIsolationAdversarialTests).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        for (var i = 0; i < 2; i++)
        {
            await using var scope = provider.CreateAsyncScope();
            var db = registration == ContextRegistration.PooledFactory
                ? provider.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext()
                : scope.ServiceProvider.GetRequiredService<TestDbContext>();
            try
            {
                if (i == 0) db.Database.EnsureCreated();
                var interceptors = Interceptors(db).OfType<OutboxDispatchTransactionInterceptor<int>>().ToList();
                Assert.Equal(sameInstance ? 1 : 2, interceptors.Count);

                // Caller transaction commit.
                using (var tx = await PoolReuseAdversarialTests.Begin(db, async))
                {
                    db.Users.Add(new User { Email = $"tx{i}@test" });
                    db.Outbox.AddEvent(new UserCreated());
                    await Save.Invoke(db, async);
                    if (async) await tx.CommitAsync(); else tx.Commit();
                }
                // Caller transaction rollback, flag=false (B1 keeps the event), then owned save.
                using (var tx = await PoolReuseAdversarialTests.Begin(db, async))
                {
                    db.Users.Add(new User { Email = $"rb{i}@test" });
                    db.Outbox.AddEvent(new UserCreated());
                    await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);
                    if (async) await tx.RollbackAsync(); else tx.Rollback();
                }
                Assert.Single(db.Outbox.GetEvents());
                await Save.Invoke(db, async);
                Assert.Empty(db.Outbox.GetEvents());
            }
            finally
            {
                if (registration == ContextRegistration.PooledFactory) db.Dispose();
            }
        }

        using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM OutboxMessages";
        Assert.Equal(4L, (long)count.ExecuteScalar()!);
        Assert.Equal(4, queue.Enqueued.Count);
        Assert.Equal(4, queue.Enqueued.Select(x => x.Id).Distinct().Count());
    }

    // ---------------- #9 plain DbContext with the interceptor ----------------

    public sealed class PlainItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class PlainContext : DbContext
    {
        public PlainContext(DbContextOptions<PlainContext> options) : base(options) { }
        public DbSet<PlainItem> Items => Set<PlainItem>();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PlainDbContextWithTheInterceptor_CommitRollbackSavepointAndFailures_DoNotThrowOrDispatch(bool async)
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var queue = new RecordingMessageQueue();
        var faults = new FaultInjector();
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddDbContext<TestDbContext>(o => o.UseSqlite(connection).ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning)));
        services.AddDbContext<PlainContext>((sp, o) => o
            .UseSqlite(connection)
            .ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning))
            .AddInterceptors(new FaultCommitInterceptor(faults), sp.GetRequiredService<OutboxDispatchTransactionInterceptor<int>>()));
        services.AddOutbox(typeof(ContextIsolationAdversarialTests).Assembly);
        services.AddEFCoreOutboxStore<TestDbContext>();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<PlainContext>();
        db.Database.EnsureCreated();
        Assert.Single(Interceptors(db).OfType<OutboxDispatchTransactionInterceptor<int>>());

        using (var tx = await PoolReuseAdversarialTests.Begin(db, async))
        {
            db.Items.Add(new PlainItem { Name = "committed" });
            await Save.Invoke(db, async);
            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            db.Items.Add(new PlainItem { Name = "undone" });
            await Save.Invoke(db, async);
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
            if (async) await tx.CommitAsync(); else tx.Commit();
        }
        db.ChangeTracker.Clear();
        using (var tx = await PoolReuseAdversarialTests.Begin(db, async))
        {
            db.Items.Add(new PlainItem { Name = "rolled-back" });
            await Save.Invoke(db, async);
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }
        db.ChangeTracker.Clear();
        faults.FailCommits();
        using (var tx = await PoolReuseAdversarialTests.Begin(db, async))
        {
            await Assert.ThrowsAsync<InjectedFailureException>(async () =>
            {
                if (async) await tx.CommitAsync(); else tx.Commit();
            });
        }
        // Owned (EF auto) transaction with two statements.
        db.Items.AddRange(new PlainItem { Name = "x" }, new PlainItem { Name = "y" });
        await Save.Invoke(db, async);

        Assert.Empty(queue.Enqueued);
    }

    internal static IReadOnlyList<IInterceptor> Interceptors(DbContext db)
        => db.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.Interceptors?.ToList() ?? new List<IInterceptor>();
}
