using Example;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Reflection;

// Run with console or worker
var asWorker = !(Debugger.IsAttached || args.Contains("--console"));

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureLogging((hostContext, config) =>
    {
        config.AddConsole();
        config.AddDebug();
    })
    .ConfigureAppConfiguration((hostContext, config) =>
    {
        config.AddJsonFile("appsettings.json", optional: true);
        config.AddJsonFile($"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json", optional: true);
        config.AddEnvironmentVariables();
        config.AddCommandLine(args);
    })
    .ConfigureHostConfiguration(config =>
    {
        config.AddEnvironmentVariables();
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.AddLogging();

        var connStrBuilder = new SqlConnectionStringBuilder(hostContext.Configuration.GetConnectionString("DefaultConnection"))
        {
            ConnectRetryCount = 5,
            Pooling = true,
            MaxPoolSize = 600,
            MinPoolSize = 5
        };
        services.AddDbContext<ApplicationDbContext>((sp, options) =>
            options.UseSqlServer(
                connStrBuilder.ToString(),
                b => b.MigrationsAssembly(typeof(ApplicationDbContext).Assembly.FullName)
                    .EnableRetryOnFailure()
                )
            );

        services.AddOutbox(Assembly.GetExecutingAssembly());
        services.AddEFCoreOutboxStore<ApplicationDbContext>();

        services.AddHostedService<AddUserHostedService>();
    });

if (asWorker)
{
    builder.Build().Run();
}
else
{
    builder.RunConsoleAsync().GetAwaiter().GetResult();
}
