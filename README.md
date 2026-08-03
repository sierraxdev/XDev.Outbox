# XDev.Outbox.EFCore

## Summary
Easy to use outbox pattern implementation for EF Core. This library provides a simple way to implement the outbox pattern in your EF Core applications, allowing you to reliably send messages or events to external systems while ensuring that your database and message broker remain in sync.

## NuGet

https://www.nuget.org/packages/XDev.Outbox.EFCore

## Setup

After installation XDev.Outbox.EFCore, extend your DbContext from OutboxDbContext:

```cs
public class ApplicationDbContext : OutboxDbContext<int>
{
    public ApplicationDbContext(
        DbContextOptions<ApplicationDbContext> options) : base(options)
    {
    }

    public DbSet<User> Users { get; set; }
}
```

Add to your dependency injection setup:

```cs
...
services.AddOutbox(Assembly.GetExecutingAssembly());
services.AddEFCoreOutboxStore<ApplicationDbContext>();
...
```

Then run EF Core migrations to create the outbox table in your database:
```bash
dotnet ef migrations add Outbox
```

### Usage

Subscribe to the outbox events in your application:

```cs
using XDev.Outbox;
public class UserCreatedEventHandler : ISubscriber<UserCreatedEvent>
{
    private readonly ILogger _logger;
    public UserCreatedEventHandler(ILogger<UserCreatedEventHandler> logger)
    {
        _logger = logger;
    }

    public async Task HandleEventAsync(UserCreatedEvent eventMessage, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("UserCreatedEventHandler received event: {message}", JsonSerializer.Serialize(eventMessage));
        await Task.Delay(1000, cancellationToken); // Simulate some processing delay
    }
}
```

Create new user logic:

```cs
var dbContext = serviceProvider.GetRequiredService<ApplicationDbContext>();
var outbox = serviceProvider.GetRequiredService<IOutbox>();
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
await dbContext.SaveChangesAsync(cancellationToken);
```
Thank you,

Daniel