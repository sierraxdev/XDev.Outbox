using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    /// <summary>
    /// Claims pending outbox messages from the <see cref="IOutboxStore{TKey}"/>, hands them to the
    /// <see cref="IMessageQueue{TKey}"/> and handles them (publish, then delete; on failure record the attempt).
    /// </summary>
    /// <remarks>
    /// Lock renewal (every third of <see cref="OutboxConfiguration.MessageLockExpirationTimeSpan"/>, never more often
    /// than every 100 ms, through <see cref="IOutboxStore{TKey}.RenewLocksAsync"/>) and poller backpressure (after a full
    /// batch, wait until it is handled, for at most the lock span, before claiming again) only happen with the built-in
    /// <see cref="MemoryMessageQueue{TKey}"/>. With any other queue the claimed locks are not renewed and the poller
    /// claims the next batch immediately. Because of the 100 ms renewal floor, a lock span below about 300 ms cannot be
    /// kept alive; renewal also needs a <c>LockExpiresAt</c> column with at least millisecond precision.
    /// </remarks>
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
        private TimeSpan MinimumRenewalInterval => TimeSpan.FromMilliseconds(100);

        /// <summary>The longest delay Task.Delay accepts on every target (including .NET Framework): int.MaxValue ms, about 24.8 days.</summary>
        private TimeSpan MaximumRenewalInterval => TimeSpan.FromMilliseconds(int.MaxValue);

        /// <summary>Failed renewal attempts remembered per message; older ones are dropped (their locks have long expired).</summary>
        private const int MaxUncertainAttempts = 8;
        private TimeSpan BackpressureCheckInterval => TimeSpan.FromSeconds(1);

        /// <summary>
        /// A message whose last renewal call failed (threw or returned null): the store may still have stored the
        /// attempted lock on its row (a statement per chunk that committed before a later one failed, or a commit whose
        /// acknowledgement was lost). Only used by the renewal loop, one tick at a time.
        /// </summary>
        private sealed class UncertainLock
        {
            public UncertainLock(DateTime expected)
            {
                Expected = expected;
            }

            /// <summary>The tracker's expected lock when the failed attempts were made.</summary>
            public DateTime Expected { get; }

            /// <summary>The lock values the failed calls tried to store, oldest first.</summary>
            public List<DateTime> Attempts { get; } = new List<DateTime>();
        }

        private readonly Dictionary<TKey, UncertainLock> _uncertainLocks = new Dictionary<TKey, UncertainLock>();

#if NET8_0_OR_GREATER
        private readonly TimeProvider _timeProvider;

        public OutboxProcessorHostedService(IMessageQueue<TKey> queue,
            ILogger<OutboxProcessorHostedService<TKey>> logger,
            IServiceScopeFactory serviceScopeFactory,
            IOptions<OutboxConfiguration> config,
            TimeProvider timeProvider)
        {
            _queue = queue;
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
            _config = config.Value;
            _timeProvider = timeProvider;
        }
        private DateTime GetUtcNow() => _timeProvider.GetUtcNow().DateTime;
#else
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

        private DateTime GetUtcNow() => DateTime.UtcNow;
#endif

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Outbox Processor Hosted Service is starting.");

            _queue.MessageReceived += HandleMessageAsync;
            await _queue.StartAsync(cancellationToken);

            // Lock renewal and poller backpressure need to know which messages the queue holds: only the built-in
            // MemoryMessageQueue provides that.
            var tracker = _queue as IMessageLockTracker<TKey>;
            var renewalTask = tracker != null ? RunLockRenewalAsync(tracker, cancellationToken) : Task.CompletedTask;
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var delayInterval = MonitorInterval;
                    try
                    {
                        _logger.LogInformation("Getting messages from store.");

                        List<OutboxMessage<TKey>> fullBatch = null;
                        using (var scope = _serviceScopeFactory.CreateScope())
                        {
                            var outboxStore = scope.ServiceProvider.GetRequiredService<IOutboxStore<TKey>>();
                            var messageQueue = scope.ServiceProvider.GetRequiredService<IMessageQueue<TKey>>();

                            var pendingMessages = (await outboxStore.GetAllAsync(ProcessorItemsCount + 1, GetUtcNow() + _config.MessageLockExpirationTimeSpan, _config?.MaxAttemps ?? DefaultMaxAttemps, cancellationToken)).ToList();

                            foreach (var item in pendingMessages)
                            {
                                await messageQueue.EnqueueAsync(item, cancellationToken);
                            }

                            if (pendingMessages.Count <= ProcessorItemsCount) await Task.Delay(MonitorInterval, cancellationToken);
                            else fullBatch = pendingMessages;
                        }

                        // Backlog: claim the next batch only once this one is handled, so claimed rows do not pile up in
                        // the queue until their locks expire.
                        if (fullBatch != null && tracker != null)
                        {
                            await WaitUntilHandledAsync(tracker, fullBatch, cancellationToken);
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
            finally
            {
                // The renewal loop ends on cancellation and never throws.
                await renewalTask;
            }
        }

        /// <summary>
        /// Waits until none of <paramref name="batch"/> is queued or being handled, checking about every second, for at
        /// most <see cref="OutboxConfiguration.MessageLockExpirationTimeSpan"/>.
        /// </summary>
        private async Task WaitUntilHandledAsync(IMessageLockTracker<TKey> tracker, List<OutboxMessage<TKey>> batch, CancellationToken cancellationToken)
        {
            var bound = _config.MessageLockExpirationTimeSpan;
            var watch = Stopwatch.StartNew();
            while (true)
            {
                if (!batch.Any(m => tracker.IsTracked(m.Id))) return;

                var remaining = bound - watch.Elapsed;
                if (remaining <= TimeSpan.Zero)
                {
                    _logger.LogInformation("The previous batch of {Count} outbox messages is still being handled after {Elapsed}; claiming the next batch.", batch.Count, watch.Elapsed);
                    return;
                }

                await Task.Delay(remaining < BackpressureCheckInterval ? remaining : BackpressureCheckInterval, cancellationToken);
            }
        }

        /// <summary>
        /// Every third of the lock span (at least 100 ms, at most about 24.8 days), extends the row locks of the
        /// messages the queue holds, so they are not claimed again while they wait or are being handled. A message
        /// whose lock could not be renewed is owned elsewhere now: its queued copy is dropped, and a running handler
        /// continues with a warning. Failures are logged and retried on the next tick (never immediately).
        /// Ends when <paramref name="cancellationToken"/> is canceled.
        /// </summary>
        private async Task RunLockRenewalAsync(IMessageLockTracker<TKey> tracker, CancellationToken cancellationToken)
        {
            var span = _config.MessageLockExpirationTimeSpan;
            var interval = TimeSpan.FromTicks(span.Ticks / 3);
            if (interval < MinimumRenewalInterval) interval = MinimumRenewalInterval;
            if (interval > MaximumRenewalInterval) interval = MaximumRenewalInterval;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(interval, cancellationToken);
                    await RenewLocksAsync(tracker, span, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to renew outbox message locks; retrying on the next tick.");

                    // Never retry immediately, whatever failed (even the interval delay itself): no hot loop.
                    try
                    {
                        await Task.Delay(MinimumRenewalInterval, cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// One renewal tick. Each message is first renewed with its expected lock. If an earlier renewal call failed,
        /// the store may have stored that call's lock on the row anyway, so a message missed with its expected lock is
        /// retried with each such attempted lock (oldest first, one call each). Last, a message still missed is retried
        /// once with this tick's own lock: a store whose statement was re-run after a commit whose acknowledgement was
        /// lost (a retrying execution strategy) has already stored it, and the re-run then missed. Only a message missed
        /// with every candidate is taken as owned elsewhere. A failed call revokes nothing; its lock is remembered as a
        /// further candidate for the messages it was sent for.
        /// </summary>
        private async Task RenewLocksAsync(IMessageLockTracker<TKey> tracker, TimeSpan span, CancellationToken cancellationToken)
        {
            var tracked = tracker.GetTrackedLocks();
            ForgetStaleUncertainLocks(tracked);
            if (tracked.Count == 0) return;

            foreach (var item in tracked)
            {
                if (item.InFlightFor.HasValue && item.InFlightFor.Value > span)
                {
                    _logger.LogWarning("Outbox message {Id} has been handled for {Elapsed}, longer than the lock span {Span}; its lock keeps being renewed while the handler runs.", item.Id, item.InFlightFor.Value, span);
                }
            }

            var lockExpiration = GetUtcNow() + span;
            var unresolved = new List<TrackedMessageLock<TKey>>(tracked);
            for (var pass = 0; unresolved.Count > 0; pass++)
            {
                var expected = new Dictionary<TKey, DateTime>();
                foreach (var item in unresolved)
                {
                    if (TryGetCandidate(item, pass, lockExpiration, out var candidate)) expected[item.Id] = candidate;
                }
                if (expected.Count == 0) break;

                IReadOnlyDictionary<TKey, DateTime> renewed;
                try
                {
                    using (var scope = _serviceScopeFactory.CreateScope())
                    {
                        var outboxStore = scope.ServiceProvider.GetRequiredService<IOutboxStore<TKey>>();
                        renewed = await outboxStore.RenewLocksAsync(expected, lockExpiration, cancellationToken);
                    }
                    if (renewed == null) throw new InvalidOperationException($"{nameof(IOutboxStore<TKey>.RenewLocksAsync)} returned null.");
                }
                catch
                {
                    // Outcome unknown: lockExpiration may now be stored on some of these rows. Nothing is revoked in this
                    // tick; the next one also accepts lockExpiration as this process's lock.
                    foreach (var item in unresolved)
                    {
                        if (expected.ContainsKey(item.Id)) RememberAttempt(item, lockExpiration);
                    }
                    throw;
                }

                var stillMissed = new List<TrackedMessageLock<TKey>>();
                foreach (var item in unresolved)
                {
                    if (expected.ContainsKey(item.Id) && renewed.TryGetValue(item.Id, out var stored))
                    {
                        _uncertainLocks.Remove(item.Id);
                        tracker.ConfirmRenewal(item.Id, item.ExpectedLock, stored);
                    }
                    else
                    {
                        stillMissed.Add(item);
                    }
                }
                unresolved = stillMissed;
            }

            // Missed with every candidate: the row was deleted or claimed elsewhere.
            foreach (var item in unresolved)
            {
                _uncertainLocks.Remove(item.Id);
                switch (tracker.Revoke(item.Id, item.ExpectedLock))
                {
                    case LockRevocation.QueuedDropped:
                        _logger.LogWarning("Outbox message {Id} lost its lock while queued (the row was claimed elsewhere or deleted); the queued copy is dropped.", item.Id);
                        break;
                    case LockRevocation.InFlight:
                        // A handler that has just deleted the row is still tracked until it returns: re-check, so a
                        // handler that finished meanwhile does not raise a false alarm.
                        if (tracker.IsTracked(item.Id))
                        {
                            _logger.LogWarning("Outbox message {Id} lost its lock while its handler is running (the row was deleted or claimed elsewhere); the handler continues without lock renewal. Unless the handler itself deleted the row, the message may be dispatched again.", item.Id);
                        }
                        else
                        {
                            _logger.LogDebug("Outbox message {Id} was not renewed because its handler had just finished.", item.Id);
                        }
                        break;
                }
            }
        }

        /// <summary>
        /// The <paramref name="pass"/>-th lock value the row of <paramref name="item"/> may hold: its expected lock, then
        /// the locks of failed renewal calls made for it since (oldest first), then this tick's own
        /// <paramref name="lockExpiration"/> (stored by a call of this tick that was re-run after it had committed).
        /// Values already tried are skipped, so a message has at most <see cref="MaxUncertainAttempts"/> + 2 candidates.
        /// </summary>
        private bool TryGetCandidate(TrackedMessageLock<TKey> item, int pass, DateTime lockExpiration, out DateTime candidate)
        {
            var index = 0;
            foreach (var value in Candidates(item, lockExpiration))
            {
                if (index++ == pass)
                {
                    candidate = value;
                    return true;
                }
            }

            candidate = default;
            return false;
        }

        private IEnumerable<DateTime> Candidates(TrackedMessageLock<TKey> item, DateTime lockExpiration)
        {
            var seen = new HashSet<DateTime> { item.ExpectedLock };
            yield return item.ExpectedLock;

            if (_uncertainLocks.TryGetValue(item.Id, out var uncertain))
            {
                foreach (var attempt in uncertain.Attempts)
                {
                    if (seen.Add(attempt)) yield return attempt;
                }
            }

            if (seen.Add(lockExpiration)) yield return lockExpiration;
        }

        private void RememberAttempt(TrackedMessageLock<TKey> item, DateTime lockExpiration)
        {
            if (!_uncertainLocks.TryGetValue(item.Id, out var uncertain))
            {
                uncertain = new UncertainLock(item.ExpectedLock);
                _uncertainLocks[item.Id] = uncertain;
            }

            // Several passes of one tick share lockExpiration.
            if (!uncertain.Attempts.Contains(lockExpiration)) uncertain.Attempts.Add(lockExpiration);

            // During a long outage one attempt is added per tick; one made 8 ticks ago (8 thirds of the span) holds a
            // lock that expired long ago.
            if (uncertain.Attempts.Count > MaxUncertainAttempts) uncertain.Attempts.RemoveAt(0);
        }

        /// <summary>
        /// Forgets failed attempts of messages no longer tracked, or whose expected lock moved since (renewed, or claimed
        /// again by this process): the row's lock is known again.
        /// </summary>
        private void ForgetStaleUncertainLocks(IReadOnlyList<TrackedMessageLock<TKey>> tracked)
        {
            if (_uncertainLocks.Count == 0) return;

            var current = new Dictionary<TKey, DateTime>(tracked.Count);
            foreach (var item in tracked) current[item.Id] = item.ExpectedLock;

            foreach (var id in _uncertainLocks.Keys.ToList())
            {
                if (!current.TryGetValue(id, out var expected) || expected != _uncertainLocks[id].Expected)
                {
                    _uncertainLocks.Remove(id);
                }
            }
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
                    var type = OutboxEventTypeResolver.Resolve(message.Type);
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
