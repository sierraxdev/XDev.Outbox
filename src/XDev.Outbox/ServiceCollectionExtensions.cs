using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using XDev.Outbox;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class ServiceCollectionExtensions
    {
        internal static Type[] allowedKeyTypes = new[] { typeof(int), typeof(Guid) };

#if NET8_0_OR_GREATER
        public static IServiceCollection AddOutbox(this IServiceCollection services, params Assembly[] assemblies)
            => services.AddOutbox<int>(assemblies, configuration: null);

        public static IServiceCollection AddOutbox<TKey>(this IServiceCollection services, params Assembly[] assemblies) where TKey : struct
            => services.AddOutbox<TKey>(assemblies, configuration: null);

        public static IServiceCollection AddOutbox<TKey>(this IServiceCollection services, Action<OutboxConfiguration>? configuration, params Assembly[] assemblies) where TKey : struct
            => services.AddOutbox<TKey>(assemblies, configuration);

        public static IServiceCollection AddOutbox<TKey>(this IServiceCollection services, IEnumerable<Assembly> assemblies, Action<OutboxConfiguration>? configuration) where TKey : struct
        {
            if (!allowedKeyTypes.Contains(typeof(TKey)))
            {
                throw new NotSupportedException($"The key type '{typeof(TKey)}' is not supported. Supported key types are: {string.Join(", ", allowedKeyTypes.Select(t => t.Name))}");
            }

            if (!assemblies.Any())
            {
                throw new ArgumentException("No assemblies found to scan. Supply at least one assembly to scan for handlers.");
            }

            if (!services.Any(d => d.ServiceType == typeof(Options.IConfigureOptions<OutboxConfiguration>)))
            {
                services.AddOptions<OutboxConfiguration>().Configure<IServiceProvider>((options, sp) => configuration?.Invoke(options));
            }

            var assignTypeFrom = typeof(ISubscriber<>);
            var subscribers = assemblies.SelectMany(x => x.GetTypes().Where(t => t.IsClass && !t.IsAbstract && (assignTypeFrom.IsAssignableFrom(t) || assignTypeFrom.IsGenericTypeDefinition && DoesTypeImplementOpenGeneric(t, assignTypeFrom))))
                .ToList();
            foreach (var subscriber in subscribers)
                foreach (var findInterface in subscriber.FindInterfaces((type, criteria) =>
                {
                    var isMatch = type.IsGenericType && ((Type)criteria).IsAssignableFrom(type.GetGenericTypeDefinition());
                    return isMatch;
                }, typeof(ISubscriber<>)))
                    services.TryAddScoped(findInterface, subscriber);

            services.TryAddScoped<IEventPublisher, EventPublisher>();
            services.TryAddScoped<IOutbox, Outbox>();
            services.TryAddSingleton<IMessageQueue<TKey>, MemoryMessageQueue<TKey>>();
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxProcessorHostedService<TKey>>());
            services.TryAddSingleton(TimeProvider.System);

            return services;
        }
#else
        public static ServiceCollection AddOutbox(this ServiceCollection services, params Assembly[] assemblies)
            => services.AddOutbox<int>(assemblies, configuration: null);

        public static ServiceCollection AddOutbox<TKey>(this ServiceCollection services, params Assembly[] assemblies) where TKey : struct
            => services.AddOutbox<TKey>(assemblies, configuration: null);

        public static ServiceCollection AddOutbox<TKey>(this ServiceCollection services, Action<OutboxConfiguration> configuration, params Assembly[] assemblies) where TKey : struct
            => services.AddOutbox<TKey>(assemblies, configuration);

        public static ServiceCollection AddOutbox<TKey>(this ServiceCollection services, IEnumerable<Assembly> assemblies, Action<OutboxConfiguration> configuration) where TKey : struct
        {
            if (!allowedKeyTypes.Contains(typeof(TKey)))
            {
                throw new NotSupportedException($"The key type '{typeof(TKey)}' is not supported. Supported key types are: {string.Join(", ", allowedKeyTypes.Select(t => t.Name))}");
            }

            if (!assemblies.Any())
            {
                throw new ArgumentException("No assemblies found to scan. Supply at least one assembly to scan for handlers.");
            }

            if (!services.Any(d => d.ServiceType == typeof(Options.IConfigureOptions<OutboxConfiguration>)))
            {
                services.AddOptions<OutboxConfiguration>().Configure<IServiceProvider>((options, sp) => configuration?.Invoke(options));
            }

            var assignTypeFrom = typeof(ISubscriber<>);
            var subscribers = assemblies.SelectMany(x => x.GetTypes().Where(t => t.IsClass && !t.IsAbstract && (assignTypeFrom.IsAssignableFrom(t) || assignTypeFrom.IsGenericTypeDefinition && DoesTypeImplementOpenGeneric(t, assignTypeFrom))))
                .ToList();
            foreach (var subscriber in subscribers)
                foreach (var findInterface in subscriber.FindInterfaces((type, criteria) =>
                {
                    var isMatch = type.IsGenericType && ((Type)criteria).IsAssignableFrom(type.GetGenericTypeDefinition());
                    return isMatch;
                }, typeof(ISubscriber<>)))
                    services.TryAddScoped(findInterface, subscriber);

            services.TryAddScoped<IEventPublisher, EventPublisher>();
            services.TryAddScoped<IOutbox, Outbox>();
            services.TryAddSingleton<IMessageQueue<TKey>, MemoryMessageQueue<TKey>>();
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxProcessorHostedService<TKey>>());

            return services;
        }
#endif

        static bool DoesTypeImplementOpenGeneric(Type type, Type openGeneric)
        {
            try
            {
                var genericTypeDefinition = openGeneric.GetGenericTypeDefinition();
                foreach (var implementedInterface in type.FindInterfaces((objType, objCriteria) => true, null))
                {
                    if (!implementedInterface.IsGenericType)
                        continue;

                    if (genericTypeDefinition.IsAssignableFrom(implementedInterface.GetGenericTypeDefinition()))
                        return true;
                }

                return false;
            }
            catch
            {
                return false;
            }
        }
    }
}
