using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Example
{
    public class DesignTimeApplicationDbContextFactory : IDesignTimeDbContextFactory<ApplicationDbContext>
    {
        public ApplicationDbContext CreateDbContext(string[] args)
        {
            // IDesignTimeDbContextFactory is used usually when you execute EF Core commands like Add-Migration, Update-Database, and so on
            // So it is usually your local development machine environment
            var envName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

            // Prepare configuration builder
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Path.Combine(Directory.GetCurrentDirectory()))
                .AddJsonFile("appsettings.json", optional: false)
            .AddJsonFile($"appsettings.{envName}.json", optional: true)
            .Build();

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddDbContext<ApplicationDbContext>((provider, options) =>
            {
                options.UseSqlServer(
                    configuration.GetConnectionString("DefaultConnection"),
                    b => b.MigrationsAssembly(typeof(ApplicationDbContext).Assembly.FullName).EnableRetryOnFailure());
            });

            serviceCollection.AddEFCoreOutboxStore<int, ApplicationDbContext>();

            var serviceProvider = serviceCollection.BuildServiceProvider();
            var scope = serviceProvider.CreateScope();
            return scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
        }
    }
}
