using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Linq;
using XDev.Outbox;
using XDev.Outbox.EFCore;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddEFCoreOutboxStore<TContext>(this IServiceCollection services, Action<OutboxDbContextConfiguration>? configuration = null) where TContext : DbContext, IOutboxDbContext<int>
            => services.AddEFCoreOutboxStore<int, TContext>(configuration);
        public static IServiceCollection AddEFCoreOutboxStore<TKey, TContext>(this IServiceCollection services, Action<OutboxDbContextConfiguration>? configuration = null) where TKey : struct where TContext : DbContext, IOutboxDbContext<TKey>
        {
            if (!services.Any(d => d.ServiceType == typeof(Options.IConfigureOptions<OutboxDbContextConfiguration>)))
            {
                services.AddOptions<OutboxDbContextConfiguration>().Configure<IServiceProvider>((options, sp) => configuration?.Invoke(options));
            }

            services.TryAddScoped<IOutboxStore<TKey>, EntityFrameworkOutboxStore<TKey, TContext>>();
            services.TryAddSingleton(TimeProvider.System);
            services.TryAddSingleton<OutboxDispatchTransactionInterceptor<TKey>>();

#if NET9_0_OR_GREATER
            services.ConfigureDbContext<TContext>((sp, options) =>
            {
                var interceptor = sp.GetRequiredService<OutboxDispatchTransactionInterceptor<TKey>>();
                if (!HasInterceptor(options.Options, interceptor))
                {
                    options.AddInterceptors(interceptor);
                }
            });

#else
            // The registered options are wrapped in place, keeping their lifetime (singleton for pooling and default
            // factories, scoped for AddDbContext), so the consumer's options action still runs on every resolution.
            var index = -1;
            for (var i = services.Count - 1; i >= 0; i--)
            {
                // Reading the implementation of a keyed descriptor throws, so keyed registrations are skipped.
                if (services[i].ServiceType == typeof(DbContextOptions<TContext>) && !services[i].IsKeyedService)
                {
                    index = i;
                    break;
                }
            }
            if (index < 0)
            {
                throw new InvalidOperationException($"No service of type '{typeof(DbContextOptions<TContext>)}' is registered. Register the DbContext '{typeof(TContext).Name}' with AddDbContext, AddDbContextPool, AddDbContextFactory or AddPooledDbContextFactory before calling AddEFCoreOutboxStore.");
            }

            var original = services[index];
            services[index] = new ServiceDescriptor(
                typeof(DbContextOptions<TContext>),
                sp => AddInterceptor(
                    (DbContextOptions<TContext>)CreateOriginalInstance(sp, original),
                    sp.GetRequiredService<OutboxDispatchTransactionInterceptor<TKey>>()),
                original.Lifetime);
#endif

            return services;
        }

        private static bool HasInterceptor(DbContextOptions options, IInterceptor interceptor)
            => options.FindExtension<CoreOptionsExtension>()?.Interceptors?.Any(x => ReferenceEquals(x, interceptor)) == true;

#if !NET9_0_OR_GREATER
        private static DbContextOptions<TContext> AddInterceptor<TContext>(DbContextOptions<TContext> options, IInterceptor interceptor)
            where TContext : DbContext
        {
            return HasInterceptor(options, interceptor)
                ? options
                : new DbContextOptionsBuilder<TContext>(options).AddInterceptors(interceptor).Options;
        }

        private static object CreateOriginalInstance(IServiceProvider serviceProvider, ServiceDescriptor descriptor)
        {
            if (descriptor.ImplementationInstance != null) return descriptor.ImplementationInstance;
            if (descriptor.ImplementationFactory != null) return descriptor.ImplementationFactory(serviceProvider);
            return ActivatorUtilities.CreateInstance(serviceProvider, descriptor.ImplementationType!);
        }
#endif
    }
}
