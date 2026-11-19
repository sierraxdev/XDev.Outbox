using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox.EFCore
{
    /// <summary>
    /// Outbox store backed by <typeparamref name="TContext"/>. Registered as a scoped service. It uses the scope's
    /// <typeparamref name="TContext"/> when one is registered (<c>AddDbContext</c>, <c>AddDbContextPool</c>,
    /// <c>AddDbContextFactory</c>); otherwise it creates a context from <see cref="IDbContextFactory{TContext}"/>
    /// (for example with <c>AddPooledDbContextFactory</c> alone) and disposes it with the scope.
    /// </summary>
    public class EntityFrameworkOutboxStore<TKey, TContext> : IOutboxStore<TKey>, IDisposable, IAsyncDisposable where TKey : struct where TContext : DbContext, IOutboxDbContext<TKey>
    {
        private readonly TContext _dbContext;
        private readonly TimeProvider _timeProvider;
        // True when the context was created from IDbContextFactory<TContext>: the store disposes it.
        private readonly bool _ownsDbContext;
        private bool _disposed;

        public EntityFrameworkOutboxStore(IServiceProvider serviceProvider, TimeProvider timeProvider)
        {
            var dbContext = serviceProvider.GetService<TContext>();
            if (dbContext == null)
            {
                var factory = serviceProvider.GetService<IDbContextFactory<TContext>>();
                if (factory == null)
                {
                    throw new InvalidOperationException($"The outbox store cannot obtain a '{typeof(TContext).Name}': neither '{typeof(TContext)}' nor '{typeof(IDbContextFactory<TContext>)}' is registered. Register the DbContext with AddDbContext, AddDbContextPool, AddDbContextFactory or AddPooledDbContextFactory.");
                }
                dbContext = factory.CreateDbContext();
                _ownsDbContext = true;
            }

            _dbContext = dbContext;
            _timeProvider = timeProvider;
        }

        public async Task<IEnumerable<OutboxMessage<TKey>>> GetAllAsync(int? itemsCount = null, DateTime? lockExpiration = null, int? maxAttemps = null, CancellationToken cancellationToken = default)
        {
            var @params = new List<SqlParameter>()
            {
                new SqlParameter("@dateTimeNow", _timeProvider.GetUtcNow().DateTime)
            };

            var queryBuilder = new StringBuilder();
            queryBuilder.Append("""
                ;WITH cte AS (
                    SELECT
                """);
            queryBuilder.AppendLine();

            if (itemsCount.HasValue)
            {
                queryBuilder.AppendLine("TOP (@take)");
                @params.Add(new SqlParameter("@take", itemsCount.Value));
            }

            queryBuilder.AppendLine($"""
                    *
                    FROM {_dbContext.GetTableNameWithScheme<OutboxMessage<TKey>>()} WITH (ROWLOCK, READPAST, UPDLOCK)
                """);
            queryBuilder.AppendLine();

            if (maxAttemps.HasValue)
            {
                queryBuilder.AppendLine("  WHERE (Attemps IS NULL OR Attemps < @maxAttemps) AND (LockExpiresAt IS NULL OR LockExpiresAt < @dateTimeNow)");
                @params.Add(new SqlParameter("@maxAttemps", maxAttemps.Value));
            }
            else
            {
                queryBuilder.AppendLine("  WHERE (LockExpiresAt IS NULL OR LockExpiresAt < @dateTimeNow)");
            }

            queryBuilder.Append("""
                        ORDER BY Id
                    )
                """);
            queryBuilder.AppendLine();

            if (lockExpiration.HasValue)
            {
                queryBuilder.Append("""
                    UPDATE cte
                    SET LockExpiresAt = @lockExpiration
                    OUTPUT INSERTED.*;
                """);
                @params.Add(new SqlParameter("@lockExpiration", lockExpiration.Value));
            } 
            else
            {
                queryBuilder.AppendLine("SELECT * FROM cte;");
            }

            var results = await _dbContext.OutboxMessages
                .FromSqlRaw(queryBuilder.ToString(), @params.ToArray())
                .AsNoTracking()
                .ToListAsync(cancellationToken);

            return results;
        }

        public Task RemoveAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
        {
            _dbContext.OutboxMessages.Remove(message);
            return _dbContext.SaveChangesAsync(cancellationToken);
        }

        public Task UpdateAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
        {
            _dbContext.OutboxMessages.Update(message);
            return _dbContext.SaveChangesAsync(cancellationToken);
        }

        /// <summary>Disposes the context when the store created it from the factory; a scope's context is left to the scope.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (_ownsDbContext)
            {
                _dbContext.Dispose();
            }
        }

        /// <inheritdoc cref="Dispose"/>
        public ValueTask DisposeAsync()
        {
            if (_disposed) return default;
            _disposed = true;
            return _ownsDbContext ? _dbContext.DisposeAsync() : default;
        }
    }
}
