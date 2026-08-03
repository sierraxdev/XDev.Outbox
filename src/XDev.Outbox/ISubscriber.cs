using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public interface ISubscriber<T>
    {
        Task HandleEventAsync(T eventMessage, CancellationToken cancellationToken = default);
    }
}
