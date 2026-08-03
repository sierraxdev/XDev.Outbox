using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
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
            services.AddOptions<OutboxDbContextConfiguration>();

            if (configuration != null)
            {
                services.Configure(configuration);
            }

            services.AddSingleton<OutboxInterceptor<TKey>>();
            services.AddScoped<IOutboxStore<TKey>, EntityFrameworkOutboxStore<TKey, TContext>>();
            services.TryAddSingleton(TimeProvider.System);

            return services;
        }
    }
}
