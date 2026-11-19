using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace XDev.Outbox.EFCore
{
    /// <summary>Helpers for the raw SQL the library builds against the outbox table.</summary>
    internal static class OutboxSqlText
    {
        /// <summary>
        /// The mapped table of <paramref name="entityType"/> delimited by the provider: the table's own schema
        /// (falling back to the model default schema), unqualified when no schema is configured, as EF's own commands.
        /// </summary>
        public static string DelimitedTableName(DbContext context, IEntityType entityType)
            => context.GetService<ISqlGenerationHelper>().DelimitIdentifier(entityType.GetTableName(), entityType.GetSchema());

        /// <summary>Escapes braces for SQL text that EF formats with parameter placeholders (FromSqlRaw, ExecuteSqlRaw).</summary>
        public static string EscapeFormat(string text) => text.Replace("{", "{{").Replace("}", "}}");
    }
}
