using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

public sealed record OutboxRow(string Id, string? Type, string? Payload, DateTime? LockExpiresAt, int? Attemps, bool? IsError, string? ErrorMessage);

/// <summary>Delimited column names of the outbox table, as the model maps them, for raw test SQL.</summary>
public sealed record OutboxColumns(string Id, string Type, string Payload, string Created, string LockExpiresAt, string Attemps, string IsError, string ErrorMessage)
{
    /// <summary>The library's default names (the property names).</summary>
    public static OutboxColumns Default { get; } = new("[Id]", "[Type]", "[Payload]", "[Created]", "[LockExpiresAt]", "[Attemps]", "[IsError]", "[ErrorMessage]");

    /// <summary>The column names <paramref name="entityType"/> maps in its own table (HasColumnName, conventions, table-specific overrides).</summary>
    public static OutboxColumns Of(IEntityType entityType)
    {
        var table = StoreObjectIdentifier.Table(entityType.GetTableName()!, entityType.GetSchema());
        string Column(string property)
        {
            var p = entityType.FindProperty(property)!;
            return "[" + (p.GetColumnName(table) ?? p.GetColumnName()).Replace("]", "]]") + "]";
        }
        return new OutboxColumns(Column("Id"), Column("Type"), Column("Payload"), Column("Created"), Column("LockExpiresAt"), Column("Attemps"), Column("IsError"), Column("ErrorMessage"));
    }
}

/// <summary>
/// A throwaway database named <c>XDevOutboxSmoke_&lt;guid&gt;</c>. Inspection helpers use their own connection in
/// autocommit mode, so do not call them while a test transaction holds locks on the rows they read.
/// </summary>
public sealed class SmokeDatabase
{
    private SmokeDatabase(string name)
    {
        Name = name;
        ConnectionString = SqlServerEnvironment.ForDatabase(name);
    }

    public string Name { get; }
    public string ConnectionString { get; }

    public static SmokeDatabase Create()
    {
        var name = SqlServerEnvironment.DatabasePrefix + Guid.NewGuid().ToString("N");
        using var connection = new SqlConnection(SqlServerEnvironment.MasterConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE [{name}]";
        command.CommandTimeout = 120;
        command.ExecuteNonQuery();
        return new SmokeDatabase(name);
    }

    public void Drop() => DropByName(Name);

    public static void DropByName(string name)
    {
        if (!SqlServerEnvironment.IsOwnDatabase(name))
        {
            throw new InvalidOperationException($"Refusing to drop '{name}': not a database created by the smoke tests.");
        }

        // Test classes run one at a time (assembly-level setting), so no other test is using a pooled connection.
        SqlConnection.ClearAllPools();

        using var connection = new SqlConnection(SqlServerEnvironment.MasterConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandTimeout = 120;
        command.CommandText = $"""
            IF DB_ID(N'{name}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{name}];
            END
            """;
        command.ExecuteNonQuery();
    }

    public SqlConnection Open()
    {
        var connection = new SqlConnection(ConnectionString);
        connection.Open();
        return connection;
    }

    public int Execute(string sql, params (string Name, object? Value)[] parameters)
    {
        using var connection = Open();
        using var command = Command(connection, sql, parameters);
        return command.ExecuteNonQuery();
    }

    public object? Scalar(string sql, params (string Name, object? Value)[] parameters)
    {
        using var connection = Open();
        using var command = Command(connection, sql, parameters);
        var value = command.ExecuteScalar();
        return value is DBNull ? null : value;
    }

    public int Count(string table) => Convert.ToInt32(Scalar($"SELECT COUNT(*) FROM {table}"));

    public List<T> Query<T>(string sql, Func<SqlDataReader, T> map, params (string Name, object? Value)[] parameters)
    {
        using var connection = Open();
        using var command = Command(connection, sql, parameters);
        using var reader = command.ExecuteReader();
        var rows = new List<T>();
        while (reader.Read()) rows.Add(map(reader));
        return rows;
    }

    /// <summary>
    /// Rows of an outbox table (delimited name, e.g. <c>[dbo].[OutboxMessages]</c>), ordered by Id. <paramref name="columns"/>
    /// gives the delimited column names when the model renames them (default: the property names).
    /// </summary>
    public List<OutboxRow> OutboxRows(string table = "[dbo].[OutboxMessages]", OutboxColumns? columns = null)
    {
        var c = columns ?? OutboxColumns.Default;
        return Query($"SELECT {c.Id}, {c.Type}, {c.Payload}, {c.LockExpiresAt}, {c.Attemps}, {c.IsError}, {c.ErrorMessage} FROM {table} ORDER BY {c.Id}", r => new OutboxRow(
            Convert.ToString(r.GetValue(0))!,
            r.IsDBNull(1) ? null : r.GetString(1),
            r.IsDBNull(2) ? null : r.GetString(2),
            r.IsDBNull(3) ? null : r.GetDateTime(3),
            r.IsDBNull(4) ? null : r.GetInt32(4),
            r.IsDBNull(5) ? null : r.GetBoolean(5),
            r.IsDBNull(6) ? null : r.GetString(6)));
    }

    public List<(int Id, string Email)> Users(string table = "[dbo].[Users]")
        => Query($"SELECT [Id], [Email] FROM {table} ORDER BY [Id]", r => (r.GetInt32(0), r.GetString(1)));

    public List<(int Id, int UserId, string Description)> Orders(string table = "[dbo].[Orders]")
        => Query($"SELECT [Id], [UserId], [Description] FROM {table} ORDER BY [Id]", r => (r.GetInt32(0), r.GetInt32(1), r.GetString(2)));

    /// <summary>
    /// Whether an outbox row is visible to another connection right now: "committed", "absent", or "locked" when an
    /// open transaction holds it (no wait: LOCK_TIMEOUT 0, locking read committed).
    /// </summary>
    public string RowState(string table, object id, string idColumn = "[Id]")
    {
        try
        {
            var count = Convert.ToInt32(Scalar($"SET LOCK_TIMEOUT 0; SELECT COUNT(*) FROM {table} WITH (READCOMMITTEDLOCK) WHERE {idColumn} = @id", ("@id", id)));
            return count == 1 ? "committed" : "absent";
        }
        catch (SqlException ex) when (ex.Number == 1222)
        {
            return "locked";
        }
    }

    /// <summary>Deletes every row of every user table (dependents first, by retrying until no FK blocks a delete).</summary>
    public void DeleteAllRows()
    {
        var tables = Query("SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id", r => r.GetString(0));
        var remaining = new List<string>(tables);
        for (var pass = 0; pass < tables.Count + 1 && remaining.Count > 0; pass++)
        {
            foreach (var table in remaining.ToList())
            {
                try
                {
                    Execute($"DELETE FROM {table}");
                    remaining.Remove(table);
                }
                catch (SqlException)
                {
                    // Referenced by a table not emptied yet; retried in the next pass.
                }
            }
        }
        if (remaining.Count > 0) throw new InvalidOperationException("Could not empty: " + string.Join(", ", remaining));
    }

    private static SqlCommand Command(SqlConnection connection, string sql, (string Name, object? Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return command;
    }
}

/// <summary>
/// Class fixture owning one throwaway database for a test class. The database is created on first use (so a class
/// whose tests are all skipped creates nothing) and dropped when the class finishes.
/// </summary>
public sealed class SmokeDatabaseFixture : IDisposable
{
    private readonly object _gate = new();
    private readonly HashSet<Type> _schemas = new();
    private SmokeDatabase? _database;

    public SmokeDatabase Database
    {
        get
        {
            lock (_gate) return _database ??= SmokeDatabase.Create();
        }
    }

    /// <summary>Creates the tables of <paramref name="context"/>'s model once per context type.</summary>
    public void EnsureTables(DbContext context)
    {
        lock (_gate)
        {
            if (!_schemas.Add(context.GetType())) return;
            context.Database.GetService<IRelationalDatabaseCreator>().CreateTables();
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _database?.Drop();
            _database = null;
        }
    }
}
