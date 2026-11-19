using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.ExceptionServices;
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
                await Invoke(method, subscriber, new object[] { @event, cancellationToken });
            }
        }

        // MethodInfo.Invoke wraps an exception thrown synchronously by the handler in TargetInvocationException;
        // rethrow the handler's own exception with its original stack trace, like an async-thrown one.
        private static Task Invoke(MethodInfo method, object subscriber, object[] arguments)
        {
            try
            {
                return (Task)method.Invoke(subscriber, arguments);
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw; // Unreachable.
            }
        }
    }
}
