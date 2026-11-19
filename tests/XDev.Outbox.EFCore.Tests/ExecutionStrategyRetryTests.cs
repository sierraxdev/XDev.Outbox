using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// A retrying execution strategy re-runs the library-owned save after a transient failure. The retried attempt
/// must produce exactly one business row, one outbox row with the real key, and one dispatch.
/// </summary>
public class ExecutionStrategyRetryTests
{
    public static TheoryData<FailurePoint, bool> Cases()
    {
        var data = new TheoryData<FailurePoint, bool>();
        foreach (var failure in Enum.GetValues<FailurePoint>())
        foreach (var async in new[] { false, true })
            data.Add(failure, async);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_WhenStepFailsOnceUnderRetryingStrategy_RetrySucceedsWithExactlyOneRowAndOneDispatch(FailurePoint failure, bool async)
    {
        using var host = OutboxTestHost.Create(retryingStrategy: true);
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        switch (failure)
        {
            case FailurePoint.OutboxInsert: host.Faults.FailCommands(SqlMatch.IsOutboxInsert); break;
            case FailurePoint.PayloadUpdate: host.Faults.FailCommands(SqlMatch.IsOutboxUpdate); break;
            case FailurePoint.Commit: host.Faults.FailCommits(); break;
        }

        await Save.Invoke(scope.Db, async);

        var fired = failure == FailurePoint.Commit ? host.Faults.CommitFailuresFired : host.Faults.CommandFailuresFired;
        Assert.True(fired == 1, $"Precondition: the injected {failure} failure must fire exactly once (fired {fired}).");

        var userId = Assert.Single(host.UserIds());
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(userId, Payload.User(row.Payload).GetProperty("id").GetInt32());

        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);

        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        Assert.Equal(userId, user.Id);
        Assert.Empty(scope.Outbox.GetEvents());
    }
}
