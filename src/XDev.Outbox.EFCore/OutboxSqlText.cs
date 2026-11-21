using System;
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
            => context.GetService<ISqlGenerationHelper>().DelimitIdentifier(RequireTableName(entityType), entityType.GetSchema());

        /// <summary>
        /// The column <paramref name="propertyName"/> of <paramref name="entityType"/> maps to in the entity's table
        /// (<c>HasColumnName</c> on the property, naming conventions), delimited by the provider. A table-specific
        /// override (<c>ToTable(..., t =&gt; t.Property(...).HasColumnName(...))</c>) is returned here too, but the claim
        /// does not support it: see <see cref="ThrowIfTableSpecificColumnNames"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// The entity is not mapped to a table (view, SQL query or function mapping), or the property is not stored in
        /// the entity's main table (entity splitting).
        /// </exception>
        public static string DelimitedColumnName(DbContext context, IEntityType entityType, string propertyName)
        {
            var table = StoreObjectIdentifier.Table(RequireTableName(entityType), entityType.GetSchema());
            var property = entityType.FindProperty(propertyName)
                ?? throw new InvalidOperationException($"The outbox entity '{entityType.DisplayName()}' has no property '{propertyName}'.");
            var column = property.GetColumnName(table)
                ?? throw new InvalidOperationException(
                    $"The outbox claim and lock renewal SQL needs '{entityType.DisplayName()}.{propertyName}' in the table " +
                    $"'{table.DisplayName()}', but it is not mapped to a column there (for example because of entity splitting). " +
                    "Map every outbox property to the outbox table.");
            return context.GetService<ISqlGenerationHelper>().DelimitIdentifier(column);
        }

        /// <summary>
        /// Throws when a mapped property of <paramref name="entityType"/> is stored in the entity's table under a column
        /// other than its default column name, i.e. a table-specific override
        /// (<c>ToTable(..., t =&gt; t.Property(...).HasColumnName(...))</c>) or a property outside the table (entity
        /// splitting). The claim reads its result with <c>FromSql</c>, which EF materializes by the default column names:
        /// the claim statement would lock the rows and then fail to read them, hiding them until the lock expires.
        /// </summary>
        /// <exception cref="InvalidOperationException">A property's column in the table differs from its default column.</exception>
        public static void ThrowIfTableSpecificColumnNames(IEntityType entityType)
        {
            var table = StoreObjectIdentifier.Table(RequireTableName(entityType), entityType.GetSchema());
            foreach (var property in entityType.GetProperties())
            {
                var defaultColumn = property.GetColumnName();
                var tableColumn = property.GetColumnName(table);
                if (string.Equals(tableColumn, defaultColumn, StringComparison.Ordinal)) continue;

                if (tableColumn == null)
                {
                    throw new InvalidOperationException(
                        $"The outbox claim needs '{entityType.DisplayName()}.{property.Name}' in the table '{table.DisplayName()}', " +
                        "but it is not mapped to a column there (for example because of entity splitting). " +
                        "Map every outbox property to the outbox table.");
                }

                throw new InvalidOperationException(
                    $"The outbox claim does not support table-specific column overrides: '{entityType.DisplayName()}.{property.Name}' " +
                    $"is stored in column '{tableColumn}' of the table '{table.DisplayName()}', but its default column name is " +
                    $"'{defaultColumn}' (EF reads the claim's result by the default column names). Configure the column name on " +
                    $"the property instead: modelBuilder.Entity<{entityType.DisplayName()}>().Property(x => x.{property.Name}).HasColumnName(\"{tableColumn}\"), " +
                    "not inside ToTable(..., t => t.Property(...).HasColumnName(...)).");
            }
        }

        /// <summary>Escapes braces for SQL text that EF formats with parameter placeholders (FromSqlRaw, ExecuteSqlRaw).</summary>
        public static string EscapeFormat(string text) => text.Replace("{", "{{").Replace("}", "}}");

        private static string RequireTableName(IEntityType entityType)
            => entityType.GetTableName()
                ?? throw new InvalidOperationException(
                    $"The outbox entity '{entityType.DisplayName()}' is not mapped to a table (for example it is mapped to a view, " +
                    "a SQL query or a function). The outbox claim and lock renewal SQL needs the outbox table.");
    }
}
