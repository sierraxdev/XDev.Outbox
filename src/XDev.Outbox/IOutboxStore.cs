using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public interface IOutboxStore<TKey> where TKey : struct
    {
        Task<IEnumerable<OutboxMessage<TKey>>> GetAllAsync(int? itemsCount = null, DateTime? lockExpiration = null, int? maxAttemps = null, CancellationToken cancellationToken = default);
        Task RemoveAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default);
        Task UpdateAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default);
    }
}
