using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox
{
    public interface IOutboxStore<TKey> where TKey : struct
    {
        Task<IEnumerable<OutboxMessage<TKey>>> GetAllAsync(int? itemsCount = null, DateTime? lockExpiration = null, int? maxAttemps = null, CancellationToken cancellationToken = default);
        Task RemoveAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default);
        Task UpdateAsync(OutboxMessage<TKey> message, CancellationToken cancellationToken = default);

        /// <summary>
        /// Extends the locks of messages this process still owns, so they are not claimed again while they wait in the
        /// queue or are being handled. Called periodically by the outbox hosted service, only when the built-in
        /// <see cref="MemoryMessageQueue{TKey}"/> is the registered queue.
        /// </summary>
        /// <remarks>
        /// <para>
        /// An Id missing from the result is treated as owned elsewhere: if the message is still queued its copy is
        /// dropped (never handled by this process), and if its handler is running it continues with a warning and its
        /// lock is no longer renewed. A store that cannot renew locks must therefore throw (for example
        /// <see cref="NotSupportedException"/>) rather than return an empty result: a thrown exception (or a null
        /// result) is logged and retried on the next tick, and nothing is dropped.
        /// </para>
        /// <para>
        /// Later calls may send, as the expected value, a lock value an earlier call tried to store (including one whose
        /// outcome was unknown because it threw, or one stored by a statement that was re-run after it had committed),
        /// and a call may send its own <paramref name="lockExpiration"/> as the expected value. Such a call renews the
        /// row to the value it already holds and returns it like any other renewal.
        /// </para>
        /// <para>
        /// The ±5 ms match needs a <c>LockExpiresAt</c> column with at least millisecond precision: SQL Server
        /// <c>datetime</c> or <c>datetime2(3)</c> or finer.
        /// </para>
        /// </remarks>
        /// <param name="expectedLocks">
        /// Message Id to the <c>LockExpiresAt</c> value the caller expects the row to hold (the value it claimed or last
        /// renewed).
        /// </param>
        /// <param name="lockExpiration">The new <c>LockExpiresAt</c> for the renewed rows.</param>
        /// <param name="cancellationToken">Cancels the operation.</param>
        /// <returns>
        /// The Ids actually renewed, each with the <c>LockExpiresAt</c> value now stored on its row (which may differ
        /// from <paramref name="lockExpiration"/> by the store's time precision). An Id is renewed only if its row still
        /// exists and its stored <c>LockExpiresAt</c> matches the expected value within ±5 ms; a row claimed by someone
        /// else since, or deleted, is left unchanged and is not returned. No row is ever inserted. An empty input
        /// returns an empty result.
        /// </returns>
        Task<IReadOnlyDictionary<TKey, DateTime>> RenewLocksAsync(IReadOnlyDictionary<TKey, DateTime> expectedLocks, DateTime lockExpiration, CancellationToken cancellationToken = default);
    }
}
