using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox.ServiceBus
{
    public class ServiceBusMessageQueue<TKey> : IAsyncDisposable where TKey : struct
    {
        readonly protected ServiceBusClient _serviceBusClient;
        readonly protected ServiceBusSender _sender;
        protected ServiceBusProcessor _serviceBusProcessor;
        readonly protected JsonSerializerOptions _jsonSerializerOptions;
        readonly protected ILogger<ServiceBusMessageQueue<TKey>> _logger;

        public event Func<OutboxMessage<TKey>, CancellationToken, Task> MessageReceived;

        public ServiceBusMessageQueue(ServiceBusClient client, IOptions<ServiceBusMessageQueueOptions> options, ILogger<ServiceBusMessageQueue<TKey>> logger)
        {
            _serviceBusClient = client;
            _sender = client.CreateSender(options.Value.QueueName);

            _jsonSerializerOptions = options.Value.SerializerOptions ?? new JsonSerializerOptions
            {
                WriteIndented = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };

            _serviceBusProcessor = _serviceBusClient.CreateProcessor(options.Value.QueueName, options.Value.ServiceBusOptions ?? new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false
            });
            _serviceBusProcessor.ProcessMessageAsync += OnProcessMessageAsync;
            _serviceBusProcessor.ProcessErrorAsync += OnProcessErrorAsync;

            _logger = logger;
        }

        public Task EnqueueAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            var data = JsonSerializer.Serialize(message, _jsonSerializerOptions);
            return SendAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(data)), cancellationToken);
        }

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Starting Service Bus processor for queue {Queue}", _serviceBusProcessor.EntityPath);
            return _serviceBusProcessor.StartProcessingAsync(cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Stopping Service Bus processor for queue {Queue}", _serviceBusProcessor.EntityPath);
            return _serviceBusProcessor.StopProcessingAsync(cancellationToken);
        }

        private async Task OnProcessMessageAsync(ProcessMessageEventArgs args)
        {
            var handler = MessageReceived;
            if (handler is null)
            {
                // No subscriber wired up — abandon so the message becomes visible again.
                await args.AbandonMessageAsync(args.Message);
                return;
            }

            try
            {
                var message = JsonSerializer.Deserialize<OutboxMessage<TKey>>(args.Message.Body, _jsonSerializerOptions);
                await handler(message, args.CancellationToken);
                await args.CompleteMessageAsync(args.Message, args.CancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Handler failed for message {MessageId}; abandoning.", args.Message.MessageId);
                await args.AbandonMessageAsync(args.Message);
            }
        }

        private Task OnProcessErrorAsync(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "Service Bus error in {ErrorSource}", args.ErrorSource);
            return Task.CompletedTask;
        }

        //public async Task<OutboxMessage<TKey>> DequeueAsync(CancellationToken cancellationToken)
        //{
        //    await _signal.WaitAsync(cancellationToken);
        //    _notifications.TryDequeue(out var message);

        //    return message;
        //}

        protected async Task SendAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
        {
            short count = 0;

            while (true)
            {
                try
                {
                    await _sender.SendMessageAsync(message, cancellationToken);
                    break;
                }
                catch (ServiceBusException ex) when (ex.IsTransient)
                {
                    if (!await WaitForRetry(count++)) throw;
                }
            }
        }

        private readonly int[] _delay = new int[] { 1000, 4000, 10000, 20000, 30000 };

        protected async Task<bool> WaitForRetry(short count)
        {
            if (count > 4) return false;

            await Task.Delay(_delay[count]);

            return true;
        }

        public async ValueTask DisposeAsync()
        {
            if (_serviceBusProcessor != null)
            {
                if(_serviceBusProcessor.IsProcessing)
                {
                    await StopAsync();
                }
                await _serviceBusProcessor.DisposeAsync();
                _serviceBusProcessor = null;
            }
        }
    }
}
