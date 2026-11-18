using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox.EFCore
{/// <summary>
 /// Tells <see cref="TransactionalOutboxMessageQueue"/> how each EF Core transaction ended: on commit the outbox
 /// messages buffered for it go to the in-memory dispatcher; on rollback, or when the commit or rollback itself fails
 /// (the outcome is unknown, and a committed row is still found by the outbox poller), they are dropped.
 /// Failures of other actions (savepoints) do not end the transaction and are ignored.
 /// </summary>
    public sealed class OutboxDispatchTransactionInterceptor<TKey> : DbTransactionInterceptor where TKey : struct
    {
        private const string CommitAction = "Commit";
        private const string RollbackAction = "Rollback";

        private readonly IMessageQueue<TKey> _queue;

        public OutboxDispatchTransactionInterceptor(IMessageQueue<TKey> queue)
        {
            _queue = queue ?? throw new ArgumentNullException(nameof(queue));
        }
        
        public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData)
        {
            var outbox = eventData.Context.GetService<IOutbox>();
            var events = outbox?.GetEvents();
            foreach (var item in eventData.Context.ChangeTracker.Entries<OutboxMessage<TKey>>()
                .Where(x => x.State == EntityState.Unchanged)
                .Select(x => x.Entity)
                .Where(x => events == null || events.Any(e => e == x.Payload))
                )
            {
                _queue.EnqueueAsync(item).GetAwaiter().GetResult();
            }
            outbox?.ClearEvents();
        }

        public override async Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
        {
            var outbox = eventData.Context.GetService<IOutbox>();
            var events = outbox?.GetEvents();
            foreach (var item in eventData.Context.ChangeTracker.Entries<OutboxMessage<TKey>>()
                .Where(x => x.State == EntityState.Unchanged)
                .Select(x => x.Entity)
                .Where(x => events == null || events.Any(e => e == x.Payload))
                )
            {
                await _queue.EnqueueAsync(item, cancellationToken);
            }
            outbox?.ClearEvents();
        }

        public override void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData)
        {
            var outbox = eventData.Context.GetService<IOutbox>();
            outbox?.ClearEvents();
        }

        public override Task TransactionRolledBackAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
        {
            var outbox = eventData.Context.GetService<IOutbox>();
            outbox?.ClearEvents();
            return Task.CompletedTask;
        }

        public override void TransactionFailed(DbTransaction transaction, TransactionErrorEventData eventData)
        {
            if (EndsTransaction(eventData))
            {
                var outbox = eventData.Context.GetService<IOutbox>();
                outbox?.ClearEvents();
            }
        }

        public override Task TransactionFailedAsync(DbTransaction transaction, TransactionErrorEventData eventData, CancellationToken cancellationToken = default)
        {
            if (EndsTransaction(eventData))
            {
                var outbox = eventData.Context.GetService<IOutbox>();
                outbox?.ClearEvents();
            }
            return Task.CompletedTask;
        }

        private static bool EndsTransaction(TransactionErrorEventData eventData) =>
            eventData.Action == CommitAction || eventData.Action == RollbackAction;
    }
}
