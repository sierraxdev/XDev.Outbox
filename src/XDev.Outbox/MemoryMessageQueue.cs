using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    /// <summary>
    /// In-process queue that hands messages to the <see cref="MessageReceived"/> handler, at most 10 at a time. A message
    /// whose Id is already queued or being handled is not added again; it can be enqueued again once its handler has
    /// returned.
    /// </summary>
    /// <remarks>
    /// Only with this queue does <see cref="OutboxProcessorHostedService{TKey}"/> renew the row locks of queued and
    /// in-flight messages (<see cref="IOutboxStore{TKey}.RenewLocksAsync"/>) and pace the poller (after a full batch it
    /// waits until that batch is handled, for at most the lock span). With a custom <see cref="IMessageQueue{TKey}"/>
    /// neither happens. Renewal runs every third of the lock span but never more often than every 100 ms, so a span below
    /// about 300 ms cannot be kept alive. The renewal match also needs a <c>LockExpiresAt</c> column with at least
    /// millisecond precision (SQL Server <c>datetime</c>, or <c>datetime2(3)</c> or finer).
    /// </remarks>
    public class MemoryMessageQueue<TKey> : IMessageQueue<TKey>, IMessageLockTracker<TKey> where TKey : struct
    {
        private readonly ILogger _logger;
        private const int WARNING_MAX_ITEMS = 300;
        private const int MaxConcurrentCalls = 10;

        /// <summary>
        /// One queued or in-flight message. Its fields are read and written under a lock on the entry. Once
        /// <see cref="Removed"/> is set the entry is no longer in <see cref="_tracked"/>.
        /// </summary>
        private sealed class Entry
        {
            public Entry(OutboxMessage<TKey> message)
            {
                Message = message;
                ExpectedLock = message.LockExpiresAt;
            }

            public OutboxMessage<TKey> Message { get; }

            /// <summary>The lock this process expects on the row; a copy, the message itself is never changed.</summary>
            public DateTime? ExpectedLock;

            /// <summary>The handler was invoked (in flight).</summary>
            public bool Started;

            /// <summary>Stopwatch timestamp when the handler was invoked.</summary>
            public long StartedAt;

            /// <summary>The row lock could not be renewed: a queued copy is skipped, a running handler continues.</summary>
            public bool Revoked;

            public bool Removed;
        }

        private readonly ConcurrentDictionary<TKey, Entry> _tracked = new ConcurrentDictionary<TKey, Entry>();
        private ConcurrentQueue<Entry> _messages = new ConcurrentQueue<Entry>();
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

            while (true)
            {
                var entry = new Entry(message);
                var existing = _tracked.GetOrAdd(message.Id, entry);
                if (ReferenceEquals(existing, entry))
                {
                    if (_messages.Count >= WARNING_MAX_ITEMS)
                    {
                        _logger.LogWarning($"Message queue reached {WARNING_MAX_ITEMS} items. Consider processing items in parallel or upgrade to a service bus or similar queue.");
                    }

                    _messages.Enqueue(entry);
                    _messageSignalSemaphore.Release();
                    return Task.CompletedTask;
                }

                lock (existing)
                {
                    // Removed between GetOrAdd and the lock: the handler has finished, so this copy is added as new.
                    if (existing.Removed) continue;

                    // A newer lock means this process claimed the row again: that is the lock to renew, and a message
                    // whose lock had been lost is owned again.
                    if (message.LockExpiresAt.HasValue && (!existing.ExpectedLock.HasValue || message.LockExpiresAt.Value > existing.ExpectedLock.Value))
                    {
                        existing.ExpectedLock = message.LockExpiresAt;
                        existing.Revoked = false;
                    }

                    _logger.LogDebug("Message {Id} is already {State}; the duplicate is not enqueued.", message.Id, existing.Started ? "being handled" : "queued");
                }
                return Task.CompletedTask;
            }
        }

        IReadOnlyList<TrackedMessageLock<TKey>> IMessageLockTracker<TKey>.GetTrackedLocks()
        {
            var result = new List<TrackedMessageLock<TKey>>();
            var now = Stopwatch.GetTimestamp();
            foreach (var pair in _tracked)
            {
                var entry = pair.Value;
                lock (entry)
                {
                    if (entry.Removed || entry.Revoked || !entry.ExpectedLock.HasValue) continue;
                    TimeSpan? inFlightFor = null;
                    if (entry.Started)
                    {
                        inFlightFor = TimeSpan.FromSeconds((now - entry.StartedAt) / (double)Stopwatch.Frequency);
                    }
                    result.Add(new TrackedMessageLock<TKey>(pair.Key, entry.ExpectedLock.Value, inFlightFor));
                }
            }
            return result;
        }

        bool IMessageLockTracker<TKey>.IsTracked(TKey id) => _tracked.ContainsKey(id);

        bool IMessageLockTracker<TKey>.ConfirmRenewal(TKey id, DateTime expectedSent, DateTime newStored)
        {
            if (!_tracked.TryGetValue(id, out var entry)) return false;
            lock (entry)
            {
                if (entry.Removed || entry.Revoked || entry.ExpectedLock != expectedSent) return false;
                entry.ExpectedLock = newStored;
                return true;
            }
        }

        LockRevocation IMessageLockTracker<TKey>.Revoke(TKey id, DateTime expectedSent)
        {
            if (!_tracked.TryGetValue(id, out var entry)) return LockRevocation.NotApplied;
            lock (entry)
            {
                if (entry.Removed || entry.Revoked || entry.ExpectedLock != expectedSent) return LockRevocation.NotApplied;
                entry.Revoked = true;
                return entry.Started ? LockRevocation.InFlight : LockRevocation.QueuedDropped;
            }
        }

        /// <summary>Removes <paramref name="entry"/> from tracking. Call under the entry's lock.</summary>
        private void Untrack(Entry entry)
        {
            entry.Removed = true;
            // Removes the key only while it still maps to this entry.
            ((ICollection<KeyValuePair<TKey, Entry>>)_tracked).Remove(new KeyValuePair<TKey, Entry>(entry.Message.Id, entry));
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
                if (_messages.TryDequeue(out var entry))
                {
                    lock (entry)
                    {
                        if (entry.Revoked)
                        {
                            Untrack(entry);
                            _logger.LogDebug("Message {Id} lost its lock while queued; it is not handled here.", entry.Message.Id);
                            return;
                        }
                        entry.Started = true;
                        entry.StartedAt = Stopwatch.GetTimestamp();
                    }

                    try
                    {
                        _logger.LogInformation($"Handling message with id {entry.Message.Id}");
                        await _messageReceived(entry.Message, cancellationToken);
                    }
                    finally
                    {
                        lock (entry)
                        {
                            Untrack(entry);
                        }
                    }
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
