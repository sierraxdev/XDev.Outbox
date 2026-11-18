using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox.EFCore
{
    public abstract class OutboxDbContext<TKey> : OutboxDbContext<TKey, OutboxDbContext<TKey>>, IOutboxDbContext<TKey> where TKey : struct
    {
        public OutboxDbContext(DbContextOptions options)
            : base(options)
        {
        }
    }
    public abstract class OutboxDbContext<TKey, TContext> : DbContext where TKey : struct where TContext : DbContext, IOutboxDbContext<TKey>
    {
        public DbSet<OutboxMessage<TKey>> OutboxMessages { get; set; }

        public OutboxDbContext(DbContextOptions options)
            : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder builder)
        {
            var internalServiceProvider = this.GetInfrastructure();
            var outboxOptions = (IOptions<OutboxDbContextConfiguration>)(internalServiceProvider.GetService(typeof(IOptions<OutboxDbContextConfiguration>))
                ?? internalServiceProvider.GetService<IDbContextOptions>()
                    ?.Extensions.OfType<CoreOptionsExtension>().FirstOrDefault()
                    ?.ApplicationServiceProvider
                    ?.GetService(typeof(IOptions<OutboxDbContextConfiguration>)));

            builder.ApplyConfiguration(new OutboxMessageConfiguration<TKey>(outboxOptions));
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
        }

        public override async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            var result = default(int);

            var outbox = this.GetService<IOutbox>();
            var messageQueue = this.GetService<IMessageQueue<TKey>>();
            var timeProvider = this.GetService<TimeProvider>();
            var outboxMessages = ToOutboxMessageEntity(outbox, timeProvider);

            if (outboxMessages.Any())
            {
                var hasExistingTransaction = Database.CurrentTransaction != null;
                if (!hasExistingTransaction)
                {
                    var entityStateSnapshot = ChangeTracker.Entries().Where(x => x.State == EntityState.Added || x.State == EntityState.Deleted || x.State == EntityState.Modified).Select(Entry => (Entry, Entry.State )).ToArray();
                    var attempt = 0;
                    var executionStrategy = Database.CreateExecutionStrategy();
                    result = await executionStrategy.ExecuteAsync(async () =>
                    {
                        await using var transaction = await Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

                        attempt++; 
                        if (attempt > 1)
                        {
                            foreach (var item in entityStateSnapshot)
                            {
                                item.Entry.State = item.State;
                                if(item.Entry.State == EntityState.Added)
                                {
                                    var property = item.Entry.Properties.FirstOrDefault(p => p.Metadata.IsPrimaryKey());
                                    if (property != null && property.Metadata.ValueGenerated == ValueGenerated.OnAdd)
                                    {
                                        property.CurrentValue = GetDefaultValue<TKey>();
                                    }
                                }
                            }
                            foreach (var item in ChangeTracker.Entries<OutboxMessage<TKey>>()
                                .Where(x => !outboxMessages.Any(m => m.Payload == x.Entity)))
                            {
                                item.State = EntityState.Detached;
                            }
                        }

                        var result = await base.SaveChangesAsync(cancellationToken);

                        var addedCount = AddOutboxMessages(outboxMessages);

                        if(addedCount > 0)
                        {
                            Database.AutoTransactionBehavior = AutoTransactionBehavior.Never;
                            await base.SaveChangesAsync(cancellationToken);
                        }

                        await transaction.CommitAsync(cancellationToken);

                        return result;
                    });
                }
                else
                {
                    result = await base.SaveChangesAsync(cancellationToken);

                    var addedCount = AddOutboxMessages(outboxMessages);
                    if (addedCount > 0)
                    {
                        Database.AutoTransactionBehavior = AutoTransactionBehavior.Never;
                        await base.SaveChangesAsync(cancellationToken);
                    }
                }
            }
            else
            {
                result = await base.SaveChangesAsync(cancellationToken);
            }

            return result;
        }

        private ICollection<OutboxMessage<TKey>> ToOutboxMessageEntity(IOutbox outbox, TimeProvider timeProvider)
        {
            var events = outbox.GetEvents();
            if(events?.Any() != true) return [];

            var outboxOptions = this.GetService<IOptions<OutboxConfiguration>>()?.Value;

            return outbox.GetEvents().Select(x => new OutboxMessage<TKey>
            {
                Id = IsPrimaryKeyAutoGenerated(this, x) ? default : GetId(),
                Type = x.GetType().AssemblyQualifiedName,
                Payload = x,
                Created = timeProvider.GetUtcNow().DateTime,
                LockExpiresAt = timeProvider.GetUtcNow().DateTime + outboxOptions.MessageLockExpirationTimeSpan
            }).ToList();
        }

        private int AddOutboxMessages(ICollection<OutboxMessage<TKey>> outboxMessages)
        {
            if (outboxMessages?.Any() != true) return 0;
            var stagedOutboxMessages = ChangeTracker.Entries<OutboxMessage<TKey>>().Select(x => x.Entity);
            var addedCount = 0;
            foreach (var item in outboxMessages.Where(x => !stagedOutboxMessages.Any(m => m.Payload == x.Payload)))
            {
                item.Id = GetDefaultValue<TKey>();
                OutboxMessages.Add(item);
                addedCount++;
            }
            return addedCount;
        }

        private static bool IsPrimaryKeyAutoGenerated<TEntity>(DbContext context, TEntity entity) where TEntity : class
        {
            // 1. Find the entity metadata structure
            var entityType = context.Model.FindEntityType(typeof(TEntity));
            if (entityType == null) return false;

            // 2. Fetch the primary key
            var primaryKey = entityType.FindPrimaryKey();
            if (primaryKey == null) return false;

            // 3. Verify if any of the key properties are generated on insert
            return primaryKey.Properties.Any(p => p.ValueGenerated == ValueGenerated.OnAdd);
        }

        private TKey GetId()
        {
            if (typeof(TKey) == typeof(string))
            {
                return (TKey)Convert.ChangeType(Guid.NewGuid().ToString(), typeof(TKey));
            }
            if (typeof(TKey) == typeof(Guid))
            {
                return (TKey)Convert.ChangeType(Guid.NewGuid(), typeof(TKey));
            }
            return default;
        }

        object GetDefaultValue(Type t)
        {
            if (t.IsValueType)
                return Activator.CreateInstance(t);
            return null;
        }

        T GetDefaultValue<T>()
        {
            return default(T);
        }
    }
}