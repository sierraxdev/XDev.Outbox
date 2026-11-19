using Microsoft.Extensions.DependencyInjection;
using System.Dynamic;
using System.Text;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.SqlServer.Tests;

/// <summary>
/// Payload typing on SQL Server: long (&gt; 4000 characters, &gt; 8000 bytes) and non-ASCII payloads through the INSERT
/// and through the payload UPDATE (an event referencing an entity with an identity key forces the UPDATE). The
/// persisted value must equal the serialized event exactly: no truncation and no lossy conversion.
/// </summary>
public class PayloadTypingTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public PayloadTypingTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public enum TextKind
    {
        Ascii,
        /// <summary>Emoji (surrogate pairs), CJK, accented Latin, quotes and backslashes.</summary>
        Unicode,
    }

    /// <summary>Text of exactly <paramref name="length"/> UTF-16 code units.</summary>
    private static string MakeText(TextKind kind, int length)
    {
        var unit = kind == TextKind.Ascii ? "abcdefghij" : "😀漢字é\"\\Ωñ🚀";
        var builder = new StringBuilder(length + unit.Length);
        while (builder.Length < length) builder.Append(unit);
        var text = builder.ToString(0, length);
        // Never cut a surrogate pair in half.
        return char.IsHighSurrogate(text[^1]) ? text[..^1] + "x" : text;
    }

    public static TheoryData<TextKind, int, bool, bool> Cases()
    {
        var data = new TheoryData<TextKind, int, bool, bool>();
        foreach (var (kind, length) in new[] { (TextKind.Ascii, 3950), (TextKind.Ascii, 4100), (TextKind.Ascii, 9000), (TextKind.Unicode, 1000), (TextKind.Unicode, 4100) })
        foreach (var throughUpdate in new[] { true, false })
        foreach (var accept in new[] { true, false })
            data.Add(kind, length, throughUpdate, accept);
        return data;
    }

    [SqlServerTheory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_LargeOrUnicodePayload_IsPersistedAndDispatchedWithoutTruncationOrConversion(TextKind kind, int length, bool throughUpdate, bool accept)
    {
        using var host = SmokeHost<int, SmokeDbContext>.Create(_fixture);
        await using var scope = host.CreateScope();
        var text = MakeText(kind, length);
        object @event;
        User? user = null;
        if (throughUpdate)
        {
            user = Graph.NewUser("typing@test");
            scope.Db.Users.Add(user);
            @event = new UserCreated { User = user, Text = text };
        }
        else
        {
            @event = new TextEvent { Text = text };
        }
        scope.Db.Outbox.AddEvent(@event);

        await scope.Db.SaveChangesAsync(accept);

        var row = Assert.Single(host.OutboxRows());
        if (user != null) user.Id = Assert.Single(host.Database.Users()).Id; // flag=false leaves the CLR key at 0
        var expected = Json.Serialize(@event);
        Assert.Equal(expected, row.Payload);
        var dataLength = Convert.ToInt64(host.Database.Scalar($"SELECT DATALENGTH([Payload]) FROM {host.OutboxTable}"));
        Assert.Equal(expected.Length * 2L, dataLength);

        var dispatched = Assert.Single(host.Queue.Enqueued);
        var dispatchedText = dispatched.Payload switch
        {
            UserCreated e => e.Text,
            TextEvent e => e.Text,
            ExpandoObject e => ((IDictionary<string, object?>)e)["text"]?.ToString(),
            var other => throw new InvalidOperationException("Unexpected payload " + other),
        };
        Assert.Equal(text, dispatchedText);

        var commands = host.Faults.Commands;
        var insert = commands.Single(c => SqlMatch.IsOutboxInsert(c.Sql));
        var insertPayload = insert.Parameters.Single(p => p.ValueLength is > 500);
        var update = commands.SingleOrDefault(c => SqlMatch.IsOutboxPayloadUpdate(c.Sql));
        Assert.Equal(throughUpdate, update != null);

        _output.WriteLine($"{kind} {length} chars, payload {expected.Length} chars / {dataLength} bytes, accept={accept}");
        _output.WriteLine($"INSERT payload parameter: {insertPayload.SqlDbType} size={insertPayload.Size} dbType={insertPayload.DbType} valueLength={insertPayload.ValueLength}");
        AssertNoTruncatingParameter(insertPayload);
        if (update != null)
        {
            _output.WriteLine("UPDATE: " + update);
            Assert.Equal(2, update.Parameters.Count);
            var payloadParameter = update.Parameters[0];
            var idParameter = update.Parameters[1];
            AssertNoTruncatingParameter(payloadParameter);
            Assert.Equal(expected.Length, payloadParameter.ValueLength);
            Assert.Equal("Int", idParameter.SqlDbType);
            Assert.DoesNotContain("CAST", update.Sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CONVERT", update.Sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    private static void AssertNoTruncatingParameter(ParameterRecord parameter)
    {
        Assert.Equal("NVarChar", parameter.SqlDbType);
        Assert.True(parameter.Size == -1 || parameter.Size >= parameter.ValueLength, $"Parameter {parameter.Name} size {parameter.Size} < value length {parameter.ValueLength}");
    }
}

/// <summary>Non-default outbox table names set with AddEFCoreOutboxStore(o =&gt; o.TableName = ...).</summary>
public class BracedTableNameTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BracedTableNameTests(SmokeDatabaseFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    private SmokeHost<int, BracedTableDbContext> CreateHost()
        => SmokeHost<int, BracedTableDbContext>.Create(_fixture, new SmokeHostOptions { StoreConfiguration = o => o.TableName = BracedTableDbContext.TableName });

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSave_TableNameWithBraces_InsertAndPayloadUpdateTargetTheConfiguredTable(bool async)
    {
        using var host = CreateHost();
        Assert.Equal("[dbo].[Outbox{Msgs}]", host.OutboxTable);
        await using var scope = host.CreateScope();
        var user = Graph.NewUser("braces@test", "b1");
        scope.Db.Users.Add(user);
        scope.Db.Outbox.AddEvent(new UserCreated { User = user });

        await Save.Invoke(scope.Db, async);

        var users = host.Database.Users();
        var row = Assert.Single(host.OutboxRows());
        GraphAssert.UserPayloadMatches(row.Payload, users, host.Database.Orders());
        var update = Assert.Single(host.Faults.Commands, c => c.Sql.StartsWith("UPDATE"));
        _output.WriteLine(update.Sql);
        Assert.StartsWith("UPDATE [Outbox{Msgs}] SET [Payload] = @p0 WHERE [Id] = @p1", update.Sql);
        Assert.Single(host.Queue.Enqueued);
    }

    [SqlServerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallerSavepointRollback_TableNameWithBraces_ExistenceQueryWorks(bool async)
    {
        using var host = CreateHost();
        await using var scope = host.CreateScope();
        await using (var tx = async ? await scope.Db.Database.BeginTransactionAsync() : scope.Db.Database.BeginTransaction())
        {
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "kept" });
            await Save.Invoke(scope.Db, async);
            if (async) await tx.CreateSavepointAsync("sp"); else tx.CreateSavepoint("sp");
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "undone" });
            await Save.Invoke(scope.Db, async);
            if (async) await tx.RollbackToSavepointAsync("sp"); else tx.RollbackToSavepoint("sp");
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.Contains("kept", row.Payload);
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id.ToString());
    }

    /// <summary>The store's claim SQL embeds the table name in a FromSqlRaw format string: it must be delimited and brace-escaped.</summary>
    [SqlServerFact]
    public async Task Store_GetAllAsync_TableNameWithBraces_ClaimsTheRow()
    {
        using var host = CreateHost();
        await using (var scope = host.CreateScope())
        {
            scope.Db.Outbox.AddEvent(new TextEvent { Text = "claim me" });
            await scope.Db.SaveChangesAsync();
        }
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = NULL");

        await using var claimScope = host.CreateScope();
        var store = claimScope.Services.GetRequiredService<IOutboxStore<int>>();
        IEnumerable<OutboxMessage<int>>? claimed = null;
        var error = await Record.ExceptionAsync(async () => claimed = await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));
        for (var e = error; e != null; e = e.InnerException) _output.WriteLine($"{e.GetType().FullName}: {e.Message}");

        Assert.Null(error);
        Assert.Single(claimed!);
    }
}

/// <summary>A plain non-default table name (valid identifier) works end to end, including the claim.</summary>
public class CustomTableNameTests : IClassFixture<SmokeDatabaseFixture>
{
    private readonly SmokeDatabaseFixture _fixture;

    public CustomTableNameTests(SmokeDatabaseFixture fixture) => _fixture = fixture;

    [SqlServerFact]
    public async Task OwnedSaveAndClaim_CustomTableName_UseTheConfiguredTable()
    {
        using var host = SmokeHost<int, CustomTableDbContext>.Create(_fixture, new SmokeHostOptions { StoreConfiguration = o => o.TableName = CustomTableDbContext.TableName });
        Assert.Equal("[dbo].[SmokeOutbox]", host.OutboxTable);
        await using (var scope = host.CreateScope())
        {
            var user = Graph.NewUser("custom@test");
            scope.Db.Users.Add(user);
            scope.Db.Outbox.AddEvent(new UserCreated { User = user });
            await scope.Db.SaveChangesAsync();
        }

        var row = Assert.Single(host.OutboxRows());
        GraphAssert.UserPayloadMatches(row.Payload, host.Database.Users(), host.Database.Orders());
        host.Database.Execute($"UPDATE {host.OutboxTable} SET [LockExpiresAt] = NULL");

        await using var claimScope = host.CreateScope();
        var store = claimScope.Services.GetRequiredService<IOutboxStore<int>>();
        var claimed = Assert.Single(await store.GetAllAsync(10, DateTime.UtcNow.AddMinutes(5), 15));
        Assert.Equal(row.Id, claimed.Id.ToString());
    }
}
