using System.Text.RegularExpressions;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// N2: a retried attempt must insert identity-keyed business entities without an explicit key value, exactly like the
/// first attempt. On SQL Server an explicit value for an IDENTITY column fails (IDENTITY_INSERT is OFF); SQLite accepts
/// it, so the shape of the executed INSERT statements is asserted instead.
/// </summary>
public class RetryInsertShapeTests
{
    private static readonly Regex InsertColumns = new(@"INSERT\s+INTO\s+""(?<table>Users|Orders)""\s*\((?<columns>[^)]*)\)", RegexOptions.IgnoreCase);

    public static TheoryData<FailurePoint, bool> Cases()
    {
        var data = new TheoryData<FailurePoint, bool>();
        foreach (var failure in new[] { FailurePoint.PayloadUpdate, FailurePoint.Commit })
        foreach (var async in new[] { false, true })
            data.Add(failure, async);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_RetriedUnderRetryingStrategy_InsertsOmitIdentityKeyColumn(FailurePoint failure, bool async)
    {
        using var host = OutboxTestHost.Create(retryingStrategy: true);
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        user.Orders.Add(new Order { Description = "o" });
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });
        Inject(host, failure);

        await Save.Invoke(scope.Db, async);

        AssertTwoAttemptsWithoutExplicitKeys(host);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_RetriedByApplication_InsertsOmitIdentityKeyColumn(FailurePoint failure, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        user.Orders.Add(new Order { Description = "o" });
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });
        Inject(host, failure);

        await Assert.ThrowsAnyAsync<Exception>(() => Save.Invoke(scope.Db, async));
        await Save.Invoke(scope.Db, async);

        AssertTwoAttemptsWithoutExplicitKeys(host);
    }

    private static void Inject(OutboxTestHost host, FailurePoint failure)
    {
        if (failure == FailurePoint.PayloadUpdate) host.Faults.FailCommands(SqlMatch.IsOutboxUpdate);
        else host.Faults.FailCommits();
    }

    private static void AssertTwoAttemptsWithoutExplicitKeys(OutboxTestHost host)
    {
        var inserts = host.Faults.ExecutedCommands
            .SelectMany(sql => InsertColumns.Matches(sql).Cast<Match>())
            .ToList();

        Assert.Equal(2, inserts.Count(m => m.Groups["table"].Value == "Users"));
        Assert.Equal(2, inserts.Count(m => m.Groups["table"].Value == "Orders"));
        Assert.All(inserts, m => Assert.DoesNotContain("\"Id\"", m.Groups["columns"].Value));

        Assert.Single(host.UserIds());
        Assert.Single(host.OrderIds());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }
}
