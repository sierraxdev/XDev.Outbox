using Microsoft.EntityFrameworkCore;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class OutboxDbContextExtensions
    {
        public static string GetTableNameWithScheme<T>(this DbContext dbContext) where T : new()
        {
            var entityType = dbContext.Model.FindEntityType(typeof(T));
            var schema = entityType.GetDefaultSchema();
            return $"{schema ?? "dbo"}.{entityType.GetTableName()}";
        }
    }
}
