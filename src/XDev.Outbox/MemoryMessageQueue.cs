using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public class MemoryMessageQueue<TKey> : IMessageQueue<TKey> where TKey : struct
    {
        private readonly ILogger _logger;
        private const int WARNING_MAX_ITEMS = 300;
        private const int MaxConcurrentCalls = 10;

        private ConcurrentQueue<OutboxMessage<TKey>> _messages = new ConcurrentQueue<OutboxMessage<TKey>>();
        private SemaphoreSlim _messageSignalSemaphore = new SemaphoreSlim(0);
        private SemaphoreSlim _subscriberSemaphore = new SemaphoreSlim(0);

        public event Func<OutboxMessage<TKey>, CancellationToken, Task> _messageReceived;
        public event Func<OutboxMessage<TKey>, CancellationToken, Task> MessageReceived
        {
            add
            {
                _messageReceived += value;
                _subscriberSemaphore.Release();
            }
            remove
            {
                _messageReceived -= value;
            }
        }

        protected CancellationTokenSource _linkedCancellationTokenSource;

        public MemoryMessageQueue(ILogger<MemoryMessageQueue<TKey>> logger)
        {
            _logger = logger;
            _concurrencyControlSemaphore = new SemaphoreSlim(MaxConcurrentCalls);
        }

        public Task EnqueueAsync(
            OutboxMessage<TKey> message, CancellationToken cancellationToken = default)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (_messages.Count >= WARNING_MAX_ITEMS)
            {
                _logger.LogWarning($"Message queue reached {WARNING_MAX_ITEMS} items. Consider processing items in parallel or upgrade to a service bus or similar queue.");
            }

            _messages.Enqueue(message);
            _messageSignalSemaphore.Release();
            return Task.CompletedTask;
        }

        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (ActiveReceiveTask == null)
            {
                _linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, CancellationToken.None);
                ActiveReceiveTask = RunReceiveTaskAsync(_linkedCancellationTokenSource.Token);
                //ActiveReceiveTask = Task.Run(() => RunReceiveTaskAsync(_linkedCancellationTokenSource.Token));
            }
            else
            {
                throw new InvalidOperationException("The message queue is already started.");
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (ActiveReceiveTask != null)
            {
                try
                {
                    _linkedCancellationTokenSource.Cancel();
                }
                catch (Exception)
                {
                }
                _linkedCancellationTokenSource.Dispose();
                _linkedCancellationTokenSource = null;
                try
                {
                    await ActiveReceiveTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Nothing to do here.  These exceptions are expected.
                }
                finally
                {
                    // If an unexpected exception occurred while awaiting the receive task, we still want to dispose and set to null
                    // as the task is complete and there is no use in awaiting it again if StopProcessingAsync is called again.
                    ActiveReceiveTask.Dispose();
                    ActiveReceiveTask = null;
                }
            }
        }

        internal readonly SemaphoreSlim _concurrencyControlSemaphore;
        private Task ActiveReceiveTask;
        internal List<Task> RunningTasks { get; private set; } = new List<Task>();
        private async Task RunReceiveTaskAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _subscriberSemaphore.WaitAsync(cancellationToken);

                while (!cancellationToken.IsCancellationRequested)
                {
                    await _messageSignalSemaphore.WaitAsync(cancellationToken);

                    await _concurrencyControlSemaphore.WaitAsync(cancellationToken);

                    RunningTasks.Add(
                            ReceiveAndProcessMessagesAsync(cancellationToken)
                        );

                    for (int i = RunningTasks.Count - 1; i >= 0; i--)
                    {
                        var task = RunningTasks[i];
                        if (task.IsCompleted)
                        {
                            RunningTasks.RemoveAt(i);
                            try
                            {
                                await task.ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "An error ocurred invoking the handler");
                            }
                        }
                    }

                    _logger.LogInformation($"{nameof(MemoryMessageQueue<TKey>)} stats => messageCount: {_messages.Count}, runningTasks: {RunningTasks.Count}");
                }
            }
            catch (OperationCanceledException)
            {
                // Nothing to do here. These exceptions are expected.
            }
            finally
            {
                try
                {
                    await Task.WhenAll(RunningTasks).ConfigureAwait(false);
                }
                finally
                {
                    _linkedCancellationTokenSource.Dispose();
                }
            }
        }
        private async Task ReceiveAndProcessMessagesAsync(
            CancellationToken cancellationToken = default)
        {
            try
            {
                if (_messages.TryDequeue(out var message))
                {
                    _logger.LogInformation($"Handling message with id {message.Id}");
                    await _messageReceived(message, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error ocurred invoking the handler");
                throw;
            }
            finally
            {
                _concurrencyControlSemaphore.Release();
            }
        }
    }
}
