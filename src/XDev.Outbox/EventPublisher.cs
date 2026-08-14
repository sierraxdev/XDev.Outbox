using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public class EventPublisher : IEventPublisher
    {
        private readonly IServiceProvider _serviceProvider;

        public EventPublisher(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public async Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        {
            var subscriberType = typeof(ISubscriber<>).MakeGenericType(@event.GetType());
            Type genericEnumerable = typeof(IEnumerable<>).MakeGenericType(subscriberType);

            foreach (var subscriber in (IEnumerable<object>)_serviceProvider.GetService(genericEnumerable))
            {
                MethodInfo method = subscriber.GetType().GetMethod(nameof(ISubscriber<TEvent>.HandleEventAsync), new Type[] { @event.GetType(), typeof(CancellationToken) });
                //MethodInfo method = subscriber.GetType().GetMethod(nameof(ISubscriber<TEvent>.HandleEventAsync));
                await (Task)method.Invoke(subscriber, new object[] { @event, cancellationToken });
            }
        }
    }
}
