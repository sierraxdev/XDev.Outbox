using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

// The tests share one SQL Server and use lock-sensitive scenarios (claims with READPAST/UPDLOCK); running them one at
// a time keeps timing assertions meaningful.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

/// <summary>
/// Where the smoke tests find SQL Server, and whether it is reachable. LocalDB is the default; the environment
/// variable <see cref="ConnectionStringVariable"/> overrides it with a base connection string (its database name is
/// ignored: each test class creates its own throwaway database).
/// </summary>
public static class SqlServerEnvironment
{
    public const string ConnectionStringVariable = "XDEV_OUTBOX_SQLSERVER";
    public const string DatabasePrefix = "XDevOutboxSmoke_";
    private const string DefaultConnectionString = @"Server=(localdb)\MSSQLLocalDB;Integrated Security=true;TrustServerCertificate=true";

    /// <summary>Databases created by an earlier, aborted run are dropped when they are at least this old.</summary>
    private static readonly TimeSpan LeftoverAge = TimeSpan.FromMinutes(30);

    private static readonly Regex OwnDatabaseName = new("^" + DatabasePrefix + "[0-9a-f]{32}$", RegexOptions.CultureInvariant);
    private static readonly Lazy<string?> Unavailable = new(Probe, LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly Lazy<string> Base = new(BuildBaseConnectionString, LazyThreadSafetyMode.ExecutionAndPublication);

    public static string BaseConnectionString => Base.Value;

    /// <summary>Null when SQL Server is reachable; otherwise why the tests are skipped.</summary>
    public static string? UnavailableReason => Unavailable.Value;

    public static string MasterConnectionString => ForDatabase("master");

    public static string ForDatabase(string database)
    {
        var builder = new SqlConnectionStringBuilder(BaseConnectionString) { InitialCatalog = database };
        return builder.ConnectionString;
    }

    /// <summary>Only databases this suite created (prefix + 32 hex digits) may ever be dropped.</summary>
    public static bool IsOwnDatabase(string name) => OwnDatabaseName.IsMatch(name);

    private static string BuildBaseConnectionString()
    {
        var configured = Environment.GetEnvironmentVariable(ConnectionStringVariable);
        var builder = new SqlConnectionStringBuilder(string.IsNullOrWhiteSpace(configured) ? DefaultConnectionString : configured)
        {
            InitialCatalog = "",
        };
        if (builder.ConnectTimeout < 15) builder.ConnectTimeout = 15;
        return builder.ConnectionString;
    }

    private static string? Probe()
    {
        string server;
        try
        {
            server = new SqlConnectionStringBuilder(BaseConnectionString).DataSource;
        }
        catch (Exception ex)
        {
            // The value is not echoed: it may contain credentials.
            return $"{ConnectionStringVariable} is not a valid SQL Server connection string ({ex.GetType().Name}). Fix or unset it to run the SQL Server smoke tests.";
        }

        try
        {
            using var connection = new SqlConnection(MasterConnectionString);
            connection.Open();
        }
        catch (Exception ex)
        {
            return $"SQL Server '{server}' is not reachable ({ex.GetType().Name}: {ex.Message}). " +
                   $"Start LocalDB or set {ConnectionStringVariable} to a base connection string to run the SQL Server smoke tests.";
        }

        try
        {
            DropLeftoverDatabases();
        }
        catch
        {
            // Cleanup of an earlier aborted run is best effort; it never decides whether the tests run.
        }
        return null;
    }

    /// <summary>
    /// Drops databases left by aborted runs. Only names created by this suite qualify, and only when older than
    /// <see cref="LeftoverAge"/>, so a concurrent run (for example another target framework) keeps its databases.
    /// </summary>
    private static void DropLeftoverDatabases()
    {
        var names = new List<string>();
        using (var connection = new SqlConnection(MasterConnectionString))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT name FROM sys.databases WHERE name LIKE @prefix AND create_date < DATEADD(MINUTE, @age, GETDATE())";
            command.Parameters.AddWithValue("@prefix", DatabasePrefix.Replace("_", "[_]") + "%");
            command.Parameters.AddWithValue("@age", -(int)LeftoverAge.TotalMinutes);
            using var reader = command.ExecuteReader();
            while (reader.Read()) names.Add(reader.GetString(0));
        }

        foreach (var name in names.Where(IsOwnDatabase))
        {
            SmokeDatabase.DropByName(name);
        }
    }
}

/// <summary>A fact that is skipped, with the reason, when SQL Server cannot be reached.</summary>
public sealed class SqlServerFactAttribute : FactAttribute
{
    public SqlServerFactAttribute()
    {
        if (SqlServerEnvironment.UnavailableReason is { } reason) Skip = reason;
    }
}

/// <summary>A theory that is skipped, with the reason, when SQL Server cannot be reached.</summary>
public sealed class SqlServerTheoryAttribute : TheoryAttribute
{
    public SqlServerTheoryAttribute()
    {
        if (SqlServerEnvironment.UnavailableReason is { } reason) Skip = reason;
    }
}
