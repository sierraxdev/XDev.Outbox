using Microsoft.Extensions.Logging;
using System.Text.Json;
using XDev.Outbox;

namespace Example
{
    public class UserCreatedEventHandler : ISubscriber<UserCreatedEvent>
    {
        protected readonly IServiceProvider _serviceProvider;
        private readonly ILogger _logger;
        public UserCreatedEventHandler(IServiceProvider serviceProvider, ILogger<UserCreatedEventHandler> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        public async Task HandleEventAsync(UserCreatedEvent eventMessage, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("UserCreatedEventHandler received event: {Email}", JsonSerializer.Serialize(eventMessage));
            await Task.Delay(1000, cancellationToken); // Simulate some processing delay
        }
    }
}
