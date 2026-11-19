using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>
/// Concurrency: N parallel scopes, each with its own context (per-context SQLite connections to a temporary file
/// database, safe for concurrent use), each saving its own entity and event. Every row's payload belongs to its own
/// scope; exactly N rows and N dispatches; no event crosses contexts or leases.
/// </summary>
public class PoolConcurrencyAdversarialTests
{
    public static TheoryData<ContextRegistration, bool> Cases()
    {
        var data = new TheoryData<ContextRegistration, bool>();
        foreach (var r in new[] { ContextRegistration.AddDbContextPool, ContextRegistration.PooledFactory, ContextRegistration.AddDbContext, ContextRegistration.Factory })
        {
            data.Add(r, false);
            data.Add(r, true);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ParallelScopes_EachSavesItsOwnEvent_RowsAndDispatchesMatchTheirScope(ContextRegistration registration, bool callerTransaction)
    {
        const int workers = 8;
        const int perWorker = 6;
        const int n = workers * perWorker;
        var path = Path.Combine(Path.GetTempPath(), $"xdev-outbox-concurrency-{Guid.NewGuid():N}.db");
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false, DefaultTimeout = 60 }.ToString();
        try
        {
            var schemaOptions = new DbContextOptionsBuilder<TestDbContext>()
                .UseSqlite(connectionString)
                .UseApplicationServiceProvider(new ServiceCollection().AddOptions().BuildServiceProvider())
                .ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning))
                .Options;
            using (var schema = new TestDbContext(schemaOptions))
            {
                schema.Database.EnsureCreated();
                schema.Database.ExecuteSqlRaw("PRAGMA journal_mode=WAL;");
            }

            var queue = new RecordingMessageQueue();
            var services = new ServiceCollection();
            services.AddSingleton<IMessageQueue<int>>(queue);
            RegistrationHost.RegisterContext(services, registration, o => o
                .UseSqlite(connectionString)
                .ConfigureWarnings(w => w.Ignore(CoreEventId.ManyServiceProvidersCreatedWarning)), poolSize: 4);
            services.AddOutbox(typeof(PoolConcurrencyAdversarialTests).Assembly);
            services.AddEFCoreOutboxStore<TestDbContext>();
            using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

            var factoryCreated = registration is ContextRegistration.PooledFactory or ContextRegistration.Factory;
            var userIdsByEmail = new ConcurrentDictionary<string, int>();
            var instances = new ConcurrentDictionary<Guid, int>();
            using var start = new ManualResetEventSlim(false);

            var tasks = Enumerable.Range(0, workers).Select(w => Task.Run(async () =>
            {
                start.Wait();
                for (var k = 0; k < perWorker; k++)
                {
                var i = w * perWorker + k;
                await using var scope = provider.CreateAsyncScope();
                var db = factoryCreated
                    ? scope.ServiceProvider.GetRequiredService<IDbContextFactory<TestDbContext>>().CreateDbContext()
                    : scope.ServiceProvider.GetRequiredService<TestDbContext>();
                try
                {
                    instances.AddOrUpdate(db.ContextId.InstanceId, 1, (_, c) => c + 1);
                    Assert.Empty(db.Outbox.GetEvents());
                    var email = $"u{i}@test";
                    var user = new User { Email = email };
                    db.Users.Add(user);
                    db.Outbox.AddEvent(new UserCreated { User = user });
                    await Task.Yield();
                    if (callerTransaction)
                    {
                        await db.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
                        {
                            await using var tx = await db.Database.BeginTransactionAsync();
                            await db.SaveChangesAsync(acceptAllChangesOnSuccess: false);
                            await tx.CommitAsync();
                        });
                        db.ChangeTracker.AcceptAllChanges();
                    }
                    else
                    {
                        await db.SaveChangesAsync();
                    }
                    Assert.Empty(db.Outbox.GetEvents());
                    Assert.True(userIdsByEmail.TryAdd(email, user.Id));
                    // Leave an unsaved event behind: must not reach any other lease.
                    db.Outbox.AddEvent(new UserCreated { User = new User { Email = $"leak{i}@test" } });
                }
                finally
                {
                    if (factoryCreated) await db.DisposeAsync();
                }
                }
            })).ToList();

            start.Set();
            await Task.WhenAll(tasks);

            if (registration is ContextRegistration.AddDbContextPool or ContextRegistration.PooledFactory)
            {
                Assert.True(instances.Values.Any(c => c > 1), "Precondition: pooled instances were reused across leases.");
            }

            using var connection = new SqliteConnection(connectionString);
            connection.Open();
            var rows = new List<(int Id, string Payload)>();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT Id, Payload FROM OutboxMessages ORDER BY Id";
                using var reader = command.ExecuteReader();
                while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1)));
            }
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT COUNT(*) FROM Users";
                Assert.Equal((long)n, (long)command.ExecuteScalar()!);
            }

            Assert.Equal(n, rows.Count);
            var payloadEmails = rows.Select(r => Payload.User(r.Payload).GetProperty("email").GetString()!).ToList();
            Assert.Equal(n, payloadEmails.Distinct().Count());
            Assert.DoesNotContain(payloadEmails, e => e.StartsWith("leak", StringComparison.Ordinal));
            foreach (var row in rows)
            {
                var user = Payload.User(row.Payload);
                Assert.Equal(userIdsByEmail[user.GetProperty("email").GetString()!], user.GetProperty("id").GetInt32());
            }

            var enqueued = queue.Enqueued;
            Assert.Equal(n, enqueued.Count);
            Assert.Equal(rows.Select(r => r.Id).OrderBy(x => x), enqueued.Select(e => e.Id).OrderBy(x => x));
            connection.Close();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in new[] { path, path + "-wal", path + "-shm" })
            {
                try { if (File.Exists(file)) File.Delete(file); } catch (IOException) { }
            }
        }
    }
}
