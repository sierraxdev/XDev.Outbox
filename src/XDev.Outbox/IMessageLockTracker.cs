using System;
using System.Collections.Generic;

namespace XDev.Outbox
{
    /// <summary>A tracked message whose row lock the hosted service keeps renewing.</summary>
    internal readonly struct TrackedMessageLock<TKey> where TKey : struct
    {
        public TrackedMessageLock(TKey id, DateTime expectedLock, TimeSpan? inFlightFor)
        {
            Id = id;
            ExpectedLock = expectedLock;
            InFlightFor = inFlightFor;
        }

        public TKey Id { get; }

        /// <summary>The lock value this process believes is stored on the row.</summary>
        public DateTime ExpectedLock { get; }

        /// <summary>How long the handler has been running; null while the message is still queued.</summary>
        public TimeSpan? InFlightFor { get; }
    }

    /// <summary>Outcome of <see cref="IMessageLockTracker{TKey}.Revoke"/>.</summary>
    internal enum LockRevocation
    {
        /// <summary>Nothing changed: the message is no longer tracked, or its expected lock moved since the snapshot.</summary>
        NotApplied,

        /// <summary>The queued copy is dropped and will not be handled.</summary>
        QueuedDropped,

        /// <summary>The handler is already running; it continues, and the lock is no longer renewed.</summary>
        InFlight,
    }

    /// <summary>
    /// Implemented by <see cref="MemoryMessageQueue{TKey}"/>: the messages it holds (queued or in flight) and the lock
    /// each one is expected to hold on its row, so the hosted service can renew those locks and pace the poller.
    /// </summary>
    internal interface IMessageLockTracker<TKey> where TKey : struct
    {
        /// <summary>Queued and in-flight messages that are not revoked and have an expected lock.</summary>
        IReadOnlyList<TrackedMessageLock<TKey>> GetTrackedLocks();

        /// <summary>Whether a message with this Id is queued or being handled.</summary>
        bool IsTracked(TKey id);

        /// <summary>Records a renewed lock, only if the expected lock is still <paramref name="expectedSent"/>.</summary>
        bool ConfirmRenewal(TKey id, DateTime expectedSent, DateTime newStored);

        /// <summary>
        /// Marks the message as no longer owned (its row lock could not be renewed), only if the expected lock is still
        /// <paramref name="expectedSent"/>. A queued copy is then skipped; a running handler continues.
        /// </summary>
        LockRevocation Revoke(TKey id, DateTime expectedSent);
    }
}
