using System;

namespace XDev.Outbox
{
    public partial class OutboxMessage<TKey> where TKey : struct
    {
        public TKey Id { get; set; }
        public string Type { get; set; }
        public object Payload { get; set; }
        public DateTime Created { get; set; }
        public DateTime? LockExpiresAt { get; set; }
        public int? Attemps { get; set; }
        public bool? IsError { get; set; }
        public string ErrorMessage { get; set; }
    }
}
