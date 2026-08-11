using Microsoft.EntityFrameworkCore;

namespace XDev.Outbox.EFCore
{
    public interface IOutboxDbContext<TKey> where TKey : struct
    {
        DbSet<OutboxMessage<TKey>> OutboxMessages { get; set; }
        string GetTableNameWithScheme<T>() where T : new();
    }
}
