using Microsoft.EntityFrameworkCore;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class OutboxDbContextExtensions
    {
        public static string GetTableNameWithScheme<T>(this DbContext dbContext) where T : new()
        {
            var entityType = dbContext.Model.FindEntityType(typeof(T));
            // The table's own schema (ToTable(name, schema)), falling back to the model default schema.
            var schema = entityType.GetSchema();
            return $"{schema ?? "dbo"}.{entityType.GetTableName()}";
        }
    }
}
