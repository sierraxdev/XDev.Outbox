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
    public class EntityFrameworkOutboxStore<TKey, TContext> : IOutboxStore<TKey> where TKey : struct where TContext : DbContext, IOutboxDbContext<TKey>
    {
        private readonly TContext _dbContext;
        private readonly TimeProvider _timeProvider;

        public EntityFrameworkOutboxStore(IServiceProvider serviceProvider, TimeProvider timeProvider)
        {
            _dbContext = serviceProvider.GetRequiredService<TContext>();
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

            var results = await _dbContext.OutboxMessage
                .FromSqlRaw(queryBuilder.ToString(), @params.ToArray())
                .AsNoTracking()
                .ToListAsync(cancellationToken);

            return results;
        }

        public Task RemoveAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
        {
            _dbContext.OutboxMessage.Remove(message);
            return _dbContext.SaveChangesAsync(cancellationToken);
        }

        public Task UpdateAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
        {
            _dbContext.OutboxMessage.Update(message);
            return _dbContext.SaveChangesAsync(cancellationToken);
        }
    }
}
