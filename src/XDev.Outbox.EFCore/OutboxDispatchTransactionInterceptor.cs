using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox.EFCore
{
    /// <summary>
    /// Tells the <see cref="IMessageQueue{TKey}"/> how each caller-owned EF Core transaction ended: on commit the outbox
    /// messages saved in it go to the in-memory dispatcher; on rollback, or when the commit or rollback itself fails
    /// (the outcome is unknown, and a committed row is still found by the outbox poller), they are dropped. Their
    /// buffered events stay buffered when they were saved with acceptAllChangesOnSuccess = false (the business changes
    /// are still pending and will be saved again) and are removed otherwise. Buffered events that were never saved in
    /// the transaction stay buffered whether it commits or not, with their still-pending business changes.
    /// An event kept for pending business changes (saved with acceptAllChangesOnSuccess = false, or never saved) stays
    /// buffered until it is saved and committed (or saved with acceptAllChangesOnSuccess = true in a transaction that
    /// does not commit, as above), also across later transactions that end, with or without a commit, without saving
    /// it; <c>Outbox.ClearEvents()</c> or the end of the context (or pool lease) discards it. A rollback to a savepoint drops the messages whose rows it undid, with the same
    /// buffer rule; rollbacks to savepoints made inside a save (EF's own, and the context's savepoint undoing a
    /// failed save with events) are ignored, since they cannot undo rows of earlier saves. When the rows a rollback
    /// to a savepoint undid cannot be determined even just before the commit, the commit proceeds, the affected messages are left to the outbox poller and a warning is logged under
    /// <c>Microsoft.EntityFrameworkCore.Database.Transaction</c> with event id 41001
    /// (<c>XDev.Outbox.EFCore.UnconfirmedPendingMessagesAtCommit</c>).
    /// The buffer itself (<see cref="OutboxDbContext{TKey, TContext}.Outbox"/>) is updated by the context
    /// (<see cref="IOutboxTransactionParticipant{TKey}"/>) for every outcome.
    /// Failures of other actions (savepoints) do not end the transaction and are ignored.
    /// Transactions owned by <see cref="OutboxDbContext{TKey, TContext}"/> dispatch their own messages and are ignored here.
    /// Transactions of contexts that do not derive from <see cref="OutboxDbContext{TKey, TContext}"/> are ignored: such
    /// a context has no event buffer.
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

        public override InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        {
            if (eventData.Context is IOutboxTransactionParticipant<TKey> participant && !participant.IsOutboxSaveInProgress)
            {
                participant.TransactionCommitting(eventData.TransactionId);
            }
            return result;
        }

        public override async ValueTask<InterceptionResult> TransactionCommittingAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            if (eventData.Context is IOutboxTransactionParticipant<TKey> participant && !participant.IsOutboxSaveInProgress)
            {
                await participant.TransactionCommittingAsync(eventData.TransactionId, cancellationToken);
            }
            return result;
        }

        public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData)
        {
            foreach (var item in TakeCommittedMessages(eventData))
            {
                _queue.EnqueueAsync(item).GetAwaiter().GetResult();
            }
        }

        public override async Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
        {
            foreach (var item in TakeCommittedMessages(eventData))
            {
                await _queue.EnqueueAsync(item, cancellationToken);
            }
        }

        public override void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData)
        {
            Discard(eventData.Context, eventData.TransactionId);
        }

        public override Task TransactionRolledBackAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
        {
            Discard(eventData.Context, eventData.TransactionId);
            return Task.CompletedTask;
        }

        public override void TransactionFailed(DbTransaction transaction, TransactionErrorEventData eventData)
        {
            if (EndsTransaction(eventData))
            {
                Discard(eventData.Context, eventData.TransactionId);
            }
        }

        public override Task TransactionFailedAsync(DbTransaction transaction, TransactionErrorEventData eventData, CancellationToken cancellationToken = default)
        {
            if (EndsTransaction(eventData))
            {
                Discard(eventData.Context, eventData.TransactionId);
            }
            return Task.CompletedTask;
        }

        public override void RolledBackToSavepoint(DbTransaction transaction, TransactionEventData eventData)
        {
            if (eventData.Context is IOutboxTransactionParticipant<TKey> participant && !participant.IsOutboxSaveInProgress)
            {
                participant.RolledBackToSavepoint(eventData.TransactionId);
            }
        }

        public override Task RolledBackToSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
        {
            if (eventData.Context is IOutboxTransactionParticipant<TKey> participant && !participant.IsOutboxSaveInProgress)
            {
                return participant.RolledBackToSavepointAsync(eventData.TransactionId, cancellationToken);
            }
            return Task.CompletedTask;
        }

        private static IReadOnlyList<OutboxMessage<TKey>> TakeCommittedMessages(TransactionEndEventData eventData)
        {
            var context = eventData.Context;
            if (context == null) return Array.Empty<OutboxMessage<TKey>>();

            if (context is IOutboxTransactionParticipant<TKey> participant)
            {
                return participant.IsOutboxSaveInProgress
                    ? Array.Empty<OutboxMessage<TKey>>()
                    : participant.TransactionCommitted(eventData.TransactionId);
            }

            return Array.Empty<OutboxMessage<TKey>>();
        }

        private static void Discard(DbContext context, Guid transactionId)
        {
            if (context is IOutboxTransactionParticipant<TKey> participant && !participant.IsOutboxSaveInProgress)
            {
                participant.EndTransactionWithoutCommit(transactionId);
            }
        }

        private static bool EndsTransaction(TransactionErrorEventData eventData) =>
            eventData.Action == CommitAction || eventData.Action == RollbackAction;
    }
}
