using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

/// <summary>
/// A failure AFTER base.SaveChanges(false) succeeded (payload UPDATE, commit) leaves store-generated values
/// (principal key and the dependent's propagated FK) pending in the tracker. The retry, whether by the
/// execution strategy or by the application, must insert principal and dependent consistently.
/// The existing retry tests only save a principal without dependents.
/// </summary>
public class DependentRetryTests
{
    public static TheoryData<FailurePoint, bool> Cases()
    {
        var data = new TheoryData<FailurePoint, bool>();
        foreach (var failure in Enum.GetValues<FailurePoint>())
        foreach (var async in new[] { false, true })
            data.Add(failure, async);
        return data;
    }

    private static void Inject(OutboxTestHost host, FailurePoint failure)
    {
        switch (failure)
        {
            case FailurePoint.OutboxInsert: host.Faults.FailCommands(SqlMatch.IsOutboxInsert); break;
            case FailurePoint.PayloadUpdate: host.Faults.FailCommands(SqlMatch.IsOutboxUpdate); break;
            case FailurePoint.Commit: host.Faults.FailCommits(); break;
        }
    }

    private static void AssertPersistedOnce(OutboxTestHost host, TestScope scope, User user)
    {
        var userId = Assert.Single(host.UserIds());
        Assert.Single(host.OrderIds());
        var row = Assert.Single(host.OutboxRows());
        var payloadUser = Payload.User(row.Payload);
        Assert.Equal(userId, payloadUser.GetProperty("id").GetInt32());
        Assert.Equal(userId, Assert.Single(payloadUser.GetProperty("orders").EnumerateArray()).GetProperty("userId").GetInt32());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
        Assert.Equal(userId, user.Id);
        Assert.Equal(userId, user.Orders[0].UserId);
        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user.Orders[0]).State);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_PrincipalWithDependent_StepFailsOnceUnderRetryingStrategy_RetryPersistsBothOnce(FailurePoint failure, bool async)
    {
        using var host = OutboxTestHost.Create(retryingStrategy: true);
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        user.Orders.Add(new Order { Description = "o" });
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });
        Inject(host, failure);

        await Save.Invoke(scope.Db, async);

        AssertPersistedOnce(host, scope, user);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_PrincipalWithDependent_StepFails_ApplicationRetryPersistsBothOnce(FailurePoint failure, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        user.Orders.Add(new Order { Description = "o" });
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });
        Inject(host, failure);

        await Assert.ThrowsAnyAsync<Exception>(() => Save.Invoke(scope.Db, async));
        Assert.Empty(host.UserIds());

        await Save.Invoke(scope.Db, async);

        AssertPersistedOnce(host, scope, user);
    }
}
