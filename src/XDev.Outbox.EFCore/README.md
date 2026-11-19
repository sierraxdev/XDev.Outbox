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

Register the DbContext (with `AddDbContext`, `AddDbContextPool`, `AddDbContextFactory` or
`AddPooledDbContextFactory`), then add the outbox to your dependency injection setup. On .NET 8 the DbContext
must be registered before `AddEFCoreOutboxStore` is called:

```cs
...
services.AddDbContext<ApplicationDbContext>(options => options.UseSqlServer(connectionString));
services.AddOutbox(Assembly.GetExecutingAssembly());
services.AddEFCoreOutboxStore<ApplicationDbContext>();
...
```

Then run EF Core migrations to create the outbox table in your database:
```bash
dotnet ef migrations add Outbox
```

### Usage

Every `OutboxDbContext` has its own event buffer, `Outbox`. Add events to the context that saves the business
changes; they are written to the outbox table in the same transaction and dispatched to the subscribers after it
commits:

```cs
var dbContext = serviceProvider.GetRequiredService<ApplicationDbContext>();
var newUser = new User
{
    Email = "testuser@example.com",
    CreatedDate = DateTime.UtcNow,
};
dbContext.Users.Add(newUser);
dbContext.Outbox.AddEvent(new UserCreatedEvent
{
    User = newUser
});
await dbContext.SaveChangesAsync(cancellationToken);
```

Subscribe to the outbox events in your application. A subscriber that publishes further events injects the
DbContext and adds them to its `Outbox`:

```cs
using XDev.Outbox;
public class UserCreatedEventHandler : ISubscriber<UserCreatedEvent>
{
    private readonly ILogger _logger;
    private readonly ApplicationDbContext _dbContext;
    public UserCreatedEventHandler(ILogger<UserCreatedEventHandler> logger, ApplicationDbContext dbContext)
    {
        _logger = logger;
        _dbContext = dbContext;
    }

    public async Task HandleEventAsync(UserCreatedEvent eventMessage, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("UserCreatedEventHandler received event: {message}", JsonSerializer.Serialize(eventMessage));
        var user = await _dbContext.Users.FirstAsync(u => u.Id == eventMessage.User.Id, cancellationToken);
        user.ModifiedDate = DateTime.UtcNow;
        _dbContext.Outbox.AddEvent(new UserUpdatedEvent { Id = user.Id });
        await _dbContext.SaveChangesAsync(cancellationToken);
    }
}
```

### Notes

- Events belong to the context they were added to: only that context's save persists them. Events that were never
  saved are discarded when the context is disposed or returned to a pool; a pooled context starts every lease with
  an empty buffer. The buffer is not thread-safe, like the context.
- `IOutbox` is no longer registered in dependency injection; use `dbContext.Outbox` instead.
- Context pooling and factories (`AddDbContextPool`, `AddDbContextFactory`, `AddPooledDbContextFactory`) are
  supported. With `AddPooledDbContextFactory` alone (no scoped DbContext registration), the outbox store creates
  its own context from the factory, so subscribers do not share the store's context.
- A context built by hand (`new ApplicationDbContext(options)`) must be given the application service provider and
  the outbox transaction interceptor:

  ```cs
  var options = new DbContextOptionsBuilder<ApplicationDbContext>()
      .UseSqlServer(connectionString)
      .UseApplicationServiceProvider(serviceProvider)
      .AddInterceptors(serviceProvider.GetRequiredService<OutboxDispatchTransactionInterceptor<int>>())
      .Options;
  ```

Thank you,

Daniel