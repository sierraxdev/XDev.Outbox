using Microsoft.EntityFrameworkCore;
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

            services.TryAddSingleton<OutboxInterceptor<TKey>>();
            services.TryAddScoped<IOutboxStore<TKey>, EntityFrameworkOutboxStore<TKey, TContext>>();
            services.TryAddSingleton(TimeProvider.System);

            return services;
        }
    }
}
