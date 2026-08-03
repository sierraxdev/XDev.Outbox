using System;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public interface IMessageQueue<TKey> where TKey : struct
    {
        Task EnqueueAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default);
        event Func<OutboxMessage<TKey>, CancellationToken, Task> MessageReceived;
        Task StartAsync(CancellationToken cancellationToken = default);
        Task StopAsync(CancellationToken cancellationToken = default);
    }
}
