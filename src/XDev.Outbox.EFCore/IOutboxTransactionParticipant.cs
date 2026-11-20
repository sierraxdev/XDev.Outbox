using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox.EFCore
{
    /// <summary>
    /// Lets <see cref="OutboxDispatchTransactionInterceptor{TKey}"/> coordinate with an <see cref="OutboxDbContext{TKey, TContext}"/>:
    /// outbox messages saved inside a caller-owned transaction are held by the context until that transaction ends.
    /// The context owns the updates of its event buffer (<see cref="OutboxDbContext{TKey, TContext}.Outbox"/>) for every
    /// transaction outcome. Every member first discards the state of an earlier pool lease (a pooled context is reused),
    /// so an event of a transaction never reaches a later lease.
    /// </summary>
    internal interface IOutboxTransactionParticipant<TKey> where TKey : struct
    {
        /// <summary>True while the context runs a save inside a transaction it owns; that save dispatches its own messages.</summary>
        bool IsOutboxSaveInProgress { get; }

        /// <summary>
        /// The given transaction is about to commit. Pending messages whose row existence could not be confirmed after a
        /// rollback to a savepoint are checked again inside the transaction; when that check fails they are left out
        /// of the in-memory dispatch (a committed row is delivered by the outbox poller) and a warning is logged with
        /// event id 41001 (<c>XDev.Outbox.EFCore.UnconfirmedPendingMessagesAtCommit</c>).
        /// </summary>
        void TransactionCommitting(Guid transactionId);

        /// <inheritdoc cref="TransactionCommitting"/>
        Task TransactionCommittingAsync(Guid transactionId, CancellationToken cancellationToken);

        /// <summary>
        /// The given transaction committed. Returns and forgets the messages saved in it that can be dispatched, and
        /// updates the event buffer: events committed in it and events never saved are removed; events
        /// of messages saved in another (ended, uncommitted) transaction follow the rule of
        /// <see cref="EndTransactionWithoutCommit"/>; events kept by an earlier release whose business changes are still
        /// pending stay buffered.
        /// </summary>
        IReadOnlyList<OutboxMessage<TKey>> TransactionCommitted(Guid transactionId);

        /// <summary>
        /// The given transaction was rolled back, or its commit or rollback failed. Forgets the messages saved in it
        /// (and any stale ones) without dispatching them, and updates the event buffer: events saved
        /// whose business changes are still pending (acceptAllChangesOnSuccess = false and not accepted by a later
        /// save) stay buffered, and so do events kept by an earlier release (in another transaction or by a rollback to
        /// a savepoint) whose business changes are still pending, and events never saved (no pending message: added
        /// after the last save or before the transaction, or their save failed), as when the transaction is disposed
        /// without a rollback; events of accepted changes are removed.
        /// </summary>
        void EndTransactionWithoutCommit(Guid transactionId);

        /// <summary>
        /// The given transaction was rolled back to a savepoint. Forgets the pending messages whose rows no longer exist
        /// and updates the event buffer for them the same way as <see cref="EndTransactionWithoutCommit"/>.
        /// When the existence check fails, the exception is rethrown and the pending messages are marked unconfirmed.
        /// </summary>
        void RolledBackToSavepoint(Guid transactionId);

        /// <inheritdoc cref="RolledBackToSavepoint"/>
        Task RolledBackToSavepointAsync(Guid transactionId, CancellationToken cancellationToken);
    }
}
