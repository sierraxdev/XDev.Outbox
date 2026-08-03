using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Dynamic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public class OutboxProcessorHostedService<TKey> : BackgroundService where TKey : struct
    {
        private readonly ILogger _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly IMessageQueue<TKey> _queue;
        private readonly OutboxConfiguration _config;
        private int ProcessorItemsCount => 100;
        private int DefaultMaxAttemps => 15;
        private TimeSpan MonitorInterval => TimeSpan.FromMinutes(30);
        private TimeSpan ErrorInterval => TimeSpan.FromMinutes(5);

        public OutboxProcessorHostedService(IMessageQueue<TKey> queue,
            ILogger<OutboxProcessorHostedService<TKey>> logger,
            IServiceScopeFactory serviceScopeFactory,
            IOptions<OutboxConfiguration> config)
        {
            _queue = queue;
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
            _config = config.Value;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Outbox Processor Hosted Service is starting.");

            _queue.MessageReceived += HandleMessageAsync;
            await _queue.StartAsync(cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
            {
                var delayInterval = MonitorInterval;
                try
                {
                    _logger.LogInformation("Getting messages from store.");

                    using (var scope = _serviceScopeFactory.CreateScope())
                    {
                        var outboxStore = scope.ServiceProvider.GetRequiredService<IOutboxStore<TKey>>();
                        var messageQueue = scope.ServiceProvider.GetRequiredService<IMessageQueue<TKey>>();

                        var pendingMessages = await outboxStore.GetAllAsync(ProcessorItemsCount + 1, DateTime.UtcNow + _config.MessageLockExpirationTimeSpan, _config?.MaxAttemps ?? DefaultMaxAttemps, cancellationToken);

                        foreach (var item in pendingMessages)
                        {
                            await messageQueue.EnqueueAsync(item, cancellationToken);
                        }

                        if (pendingMessages.Count() <= ProcessorItemsCount) await Task.Delay(MonitorInterval, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred getting messages from store.");
                    await Task.Delay(ErrorInterval, cancellationToken);
                }
            }

            await _queue.StopAsync(cancellationToken);
            _logger.LogInformation("Outbox Processor Hosted Service is stopping.");
        }

        private async Task HandleMessageAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken)
        {
            if (message == null) return;

            using (var scope = _serviceScopeFactory.CreateScope())
            {
                var outboxStore = scope.ServiceProvider.GetRequiredService<IOutboxStore<TKey>>();
                var eventPublisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

                try
                {
                    var type = Type.GetType(message.Type);
                    if (type == null) throw new Exception($"Failed to load type {message.Type}");

                    object payloadObj = message.Payload;
                    if (message.Payload is ExpandoObject)
                        payloadObj = (message.Payload as ExpandoObject)?.ToObject(type);
                    else if (message.Payload.GetType() == typeof(JsonElement))
                        payloadObj = ((JsonElement)message.Payload).ToObject(type);

                    if (payloadObj == null) throw new Exception($"Failed to convert payload to type {message.Type} for message {message.Id}");

                    await eventPublisher.PublishAsync(payloadObj, cancellationToken);

                    await outboxStore.RemoveAsync(message, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Handler failed for message {Id}; abandoning.", message.Id);

                    message.Attemps = (message.Attemps ?? 0) + 1;
                    message.ErrorMessage = ex.Message;
                    message.IsError = true;
                    await outboxStore.UpdateAsync(message, cancellationToken);

                    throw;
                }
            }
        }
    }
}
