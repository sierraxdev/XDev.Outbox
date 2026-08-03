using System.Collections.Generic;

namespace XDev.Outbox
{
    public interface IOutbox
    {
        void AddEvent(params object[] events);
        IReadOnlyCollection<object> GetEvents();
        void ClearEvents();
    }
}
