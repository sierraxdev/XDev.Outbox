using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Collections.Generic;
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
            services.TryAddSingleton<OutboxDispatchTransactionInterceptor<TKey>>();

#if NET9_0_OR_GREATER
            services.ConfigureDbContext<TContext>((sp, options) =>
                options.AddInterceptors(sp.GetRequiredService<OutboxDispatchTransactionInterceptor<TKey>>()));

#else
            var contextOptions = services.FirstOrDefault(x => x.ServiceType == typeof(DbContextOptions<TContext>));
            if (contextOptions == null)
            {
                throw new InvalidOperationException($"No service of type '{typeof(DbContextOptions<TContext>)}' found. Ensure that the DbContext '{typeof(TContext).Name}' is registered before calling 'AddEFCoreOutboxStore'.");
            }
            var dbContextOptions = services.BuildServiceProvider().GetService<DbContextOptions<TContext>>();
            services.Replace(new ServiceDescriptor(
                    typeof(DbContextOptions<TContext>),
                    p => CreateDbContextOptions(p, dbContextOptions, (sp, options) =>
                                options.AddInterceptors(sp.GetRequiredService<OutboxDispatchTransactionInterceptor<TKey>>())),
                    ServiceLifetime.Scoped));
#endif

            return services;
        }

#if !NET9_0_OR_GREATER
        private static DbContextOptions<TContext> CreateDbContextOptions<TContext>(
            IServiceProvider applicationServiceProvider,
            DbContextOptions<TContext>? baseOptions,
            Action<IServiceProvider, DbContextOptionsBuilder>? optionsAction)
            where TContext : DbContext
        {
            var builder = new DbContextOptionsBuilder<TContext>(
                baseOptions ?? new DbContextOptions<TContext>(new Dictionary<Type, IDbContextOptionsExtension>()));

            builder.UseApplicationServiceProvider(applicationServiceProvider);

            optionsAction?.Invoke(applicationServiceProvider, builder);

            return builder.Options;
        }
#endif
    }
}
