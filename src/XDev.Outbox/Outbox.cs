using System.Collections.Generic;

namespace XDev.Outbox
{
    public class Outbox : IOutbox
    {
        private List<object> _events { get; } = new List<object>();

        public IReadOnlyCollection<object> GetEvents()
        {
            return _events.AsReadOnly();
        }

        public void AddEvent(params object[] events)
        {
            _events.AddRange(events);
        }

        public void ClearEvents()
        {
            _events.Clear();
        }
    }
}
