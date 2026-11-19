using System.Collections.Generic;

namespace XDev.Outbox
{
    /// <summary>
    /// A buffer of events to persist with the next save of the database context that owns it. With
    /// XDev.Outbox.EFCore, every <c>OutboxDbContext</c> exposes its own buffer through its <c>Outbox</c> property
    /// (for example <c>dbContext.Outbox.AddEvent(...)</c>); the buffer is not registered in dependency injection.
    /// Implementations are not thread-safe.
    /// </summary>
    public interface IOutbox
    {
        /// <summary>Adds events, in order, to be persisted with the owner's next save.</summary>
        void AddEvent(params object[] events);

        /// <summary>The buffered events, in the order they were added.</summary>
        IReadOnlyCollection<object> GetEvents();

        /// <summary>Removes every buffered event; they will not be persisted.</summary>
        void ClearEvents();
    }
}
