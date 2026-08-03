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

            while (!stoppingToken.IsCancellationRequested)
            {
                using (var scope = _serviceScopeFactory.CreateScope())
                {
                    var scopedServices = scope.ServiceProvider;
                    var dbContext = scopedServices.GetRequiredService<ApplicationDbContext>();
                    var outbox = scopedServices.GetRequiredService<IOutbox>();

                    try
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

                        //var executionStrategy = dbContext.Database.CreateExecutionStrategy();
                        //await executionStrategy.ExecuteAsync(async () =>
                        //{
                        //    using (var dbContextTransaction = dbContext.Database.BeginTransaction())
                        //    {
                        //        await dbContext.SaveChangesAsync(stoppingToken);

                        //        dbContextTransaction.Commit();
                        //    }
                        //});

                        await dbContext.SaveChangesAsync(stoppingToken);

                        _logger.LogInformation("User Created");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                           "Error occurred executing create user.");
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }

            _logger.LogInformation("Service is stopped");
        }
    }
}
