using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public interface IEventPublisher
    {
        Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default);
    }
}
