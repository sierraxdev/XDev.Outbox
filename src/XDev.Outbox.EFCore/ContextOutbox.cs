using System;
using System.Collections.Generic;

namespace XDev.Outbox.EFCore
{
    /// <summary>
    /// The event buffer of one <see cref="OutboxDbContext{TKey, TContext}"/> (<see cref="OutboxDbContext{TKey, TContext}.Outbox"/>).
    /// Every public member first lets the context discard the state of an earlier pool lease, so events added while
    /// the context was leased before (and never saved) are not seen by the current lease. Not thread-safe, like the
    /// context itself.
    /// </summary>
    internal sealed class ContextOutbox : IOutbox
    {
        private readonly Action _ensureCurrentLease;

        public ContextOutbox(Action ensureCurrentLease)
        {
            _ensureCurrentLease = ensureCurrentLease ?? throw new ArgumentNullException(nameof(ensureCurrentLease));
        }

        /// <summary>
        /// The buffered events, in the order they were added; used directly by the owning context, which checks the
        /// lease itself before using it.
        /// </summary>
        internal List<object> Events { get; } = new List<object>();

        public void AddEvent(params object[] events)
        {
            _ensureCurrentLease();
            Events.AddRange(events);
        }

        public IReadOnlyCollection<object> GetEvents()
        {
            _ensureCurrentLease();
            return Events.AsReadOnly();
        }

        public void ClearEvents()
        {
            _ensureCurrentLease();
            Events.Clear();
        }
    }
}
