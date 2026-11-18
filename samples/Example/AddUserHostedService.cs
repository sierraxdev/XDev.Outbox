using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using XDev.Outbox;

namespace Example
{
    public class AddUserHostedService : BackgroundService
    {
        private readonly ILogger _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;

        public AddUserHostedService(ILogger<AddUserHostedService> logger,
            IServiceScopeFactory serviceScopeFactory)
        {
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Service is started");

            var useStrategy = false;
            var useNewDbContext = false;
            TransientFailureSimulator.SimulateFailureOnce = false;

            while (!stoppingToken.IsCancellationRequested)
            {
                await using var scope = _serviceScopeFactory.CreateAsyncScope();

                var scopedServices = scope.ServiceProvider;
                var dbContext = scopedServices.GetRequiredService<ApplicationDbContext>();
                var dbContextFactory = scopedServices.GetService<IDbContextFactory<ApplicationDbContext>>();
                var outbox = scopedServices.GetRequiredService<IOutbox>();

                try
                {
                    if(useStrategy)
                    {

                        var executionStrategy = dbContext.Database.CreateExecutionStrategy();
                        await executionStrategy.ExecuteAsync(async () =>
                        {
                            if (useNewDbContext)
                            {
                                if (dbContextFactory != null)
                                {
                                    await using var context = await dbContextFactory.CreateDbContextAsync(stoppingToken);
                                    await using var contextTransaction = await dbContext.Database.BeginTransactionAsync(stoppingToken);

                                    var newUser = new User
                                    {
                                        Email = "testuser@example.com",
                                        CreatedDate = DateTime.UtcNow,
                                    };
                                    context.Users.Add(newUser);
                                    outbox.AddEvent(new UserCreatedEvent
                                    {
                                        User = newUser
                                    });

                                    await context.SaveChangesAsync(stoppingToken);

                                    if (TransientFailureSimulator.SimulateFailureOnce)
                                    {
                                        TransientFailureSimulator.SimulateFailureOnce = false;
                                        Console.WriteLine("Simulating transient failure...");
                                        throw new TimeoutException("Simulated transient failure");
                                    }

                                    await contextTransaction.CommitAsync(stoppingToken);
                                }
                                else
                                {
                                    await using var scopeStrategy = _serviceScopeFactory.CreateAsyncScope();
                                    var context = scopeStrategy.ServiceProvider.GetRequiredService<ApplicationDbContext>();
                                    var outboxStrategy = scopeStrategy.ServiceProvider.GetRequiredService<IOutbox>();

                                    await using var contextTransaction = await dbContext.Database.BeginTransactionAsync(stoppingToken);

                                    var newUser = new User
                                    {
                                        Email = "testuser@example.com",
                                        CreatedDate = DateTime.UtcNow,
                                    };
                                    context.Users.Add(newUser);
                                    outboxStrategy.AddEvent(new UserCreatedEvent
                                    {
                                        User = newUser
                                    });

                                    await context.SaveChangesAsync(stoppingToken);

                                    if (TransientFailureSimulator.SimulateFailureOnce)
                                    {
                                        TransientFailureSimulator.SimulateFailureOnce = false;
                                        Console.WriteLine("Simulating transient failure...");
                                        throw new TimeoutException("Simulated transient failure");
                                    }

                                    await contextTransaction.CommitAsync(stoppingToken);
                                }
                            }
                            else
                            {
                                dbContext.ChangeTracker.Clear();
                                outbox.ClearEvents();

                                await using var contextTransaction = await dbContext.Database.BeginTransactionAsync(stoppingToken);

                                var newUser = new User
                                {
                                    Email = "testuser@example.com",
                                    CreatedDate = DateTime.UtcNow,
                                };
                                dbContext.Users.Add(newUser);
                                outbox.AddEvent(new UserCreatedEvent
                                {
                                    User = newUser
                                });

                                await dbContext.SaveChangesAsync(stoppingToken);

                                if (TransientFailureSimulator.SimulateFailureOnce)
                                {
                                    TransientFailureSimulator.SimulateFailureOnce = false;
                                    Console.WriteLine("Simulating transient failure...");
                                    throw new TimeoutException("Simulated transient failure");
                                }

                                await contextTransaction.CommitAsync(stoppingToken);
                            }
                        });
                    }
                    else
                    {
                        var newUser = new User
                        {
                            Email = "testuser@example.com",
                            CreatedDate = DateTime.UtcNow,
                        };
                        dbContext.Users.Add(newUser);
                        outbox.AddEvent(new UserCreatedEvent
                        {
                            User = newUser
                        });

                        await dbContext.SaveChangesAsync(stoppingToken);
                    }

                    _logger.LogInformation("User Created");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Error occurred executing create user.");
                }

                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }

            _logger.LogInformation("Service is stopped");
        }

        public static class TransientFailureSimulator
        {
            public static bool SimulateFailureOnce { get; set; } = false;
        }
    }
}
