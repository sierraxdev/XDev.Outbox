using Microsoft.Extensions.Logging;
using System.Text.Json;
using XDev.Outbox;

namespace Example
{
    public class UserUpdatedEventHandler : ISubscriber<UserUpdatedEvent>
    {
        private readonly ILogger _logger;
        public UserUpdatedEventHandler(ILogger<UserUpdatedEventHandler> logger)
        {
            _logger = logger;
        }

        public async Task HandleEventAsync(UserUpdatedEvent eventMessage, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("UserUpdatedEventHandler received event: {Email}", JsonSerializer.Serialize(eventMessage));
        }
    }
}
