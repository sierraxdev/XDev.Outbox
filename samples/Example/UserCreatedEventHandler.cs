using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using XDev.Outbox;

namespace Example
{
    public class UserCreatedEventHandler : ISubscriber<UserCreatedEvent>
    {
        protected readonly IServiceProvider _serviceProvider;
        private readonly ILogger _logger;
        protected readonly ApplicationDbContext _applicationDbContext;
        private readonly IOutbox _outbox;
        public UserCreatedEventHandler(IServiceProvider serviceProvider, ILogger<UserCreatedEventHandler> logger,
            ApplicationDbContext applicationDbContext, IOutbox outbox)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _applicationDbContext = applicationDbContext;
            _outbox = outbox;
        }

        public async Task HandleEventAsync(UserCreatedEvent eventMessage, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("UserCreatedEventHandler received event: {Email}", JsonSerializer.Serialize(eventMessage));
            var user = await _applicationDbContext.Users.Where(u => u.Id == eventMessage.User.Id).FirstOrDefaultAsync(cancellationToken);
            if (user != null)
            {
                user.ModifiedDate = DateTime.UtcNow;
                _outbox.AddEvent(new UserUpdatedEvent { Id = user.Id });
                await _applicationDbContext.SaveChangesAsync(cancellationToken);
            }
        }
    }
}
