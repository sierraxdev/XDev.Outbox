using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// Finding #1: every public save overload must persist buffered events atomically with the business
/// changes and dispatch them after commit.
/// </summary>
public class SaveOverloadTests
{
    public static TheoryData<SaveCall, int> OverloadsAndStatementCounts()
    {
        var data = new TheoryData<SaveCall, int>();
        foreach (var call in Enum.GetValues<SaveCall>())
        {
            data.Add(call, 1);
            data.Add(call, 2);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(OverloadsAndStatementCounts))]
    public async Task Save_WithBufferedEvent_WritesOneOutboxRowAndEnqueuesItOnceAfterCommit(SaveCall call, int businessStatements)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        if (businessStatements == 2)
        {
            user.Orders.Add(new Order { Description = "first order" });
        }
        scope.Db.Users.Add(user);
        var evt = new UserCreated { User = user };
        scope.Outbox.AddEvent(evt);

        await Save.Invoke(scope.Db, call);

        Assert.Equal(1, host.Count("Users"));
        Assert.Equal(businessStatements == 2 ? 1 : 0, host.Count("Orders"));

        var rows = host.OutboxRows();
        Assert.Single(rows);
        Assert.Equal(typeof(UserCreated).AssemblyQualifiedName, rows[0].Type);

        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.NotEqual(0, enqueued.Id);
        Assert.Equal(rows[0].Id, enqueued.Id);

        Assert.Empty(scope.Outbox.GetEvents());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_WithNoEventsAndAcceptFalse_LeavesEntriesAddedAndWritesNoOutbox(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);

        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);

        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
        Assert.Equal(1, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(SaveCall.SaveChanges)]
    [InlineData(SaveCall.SaveChangesAsync)]
    public async Task Save_WithNoEvents_WritesBusinessRowsOnlyAndEnqueuesNothing(SaveCall call)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        // Two tables => EF opens its own transaction, so the dispatch interceptor's commit hook runs.
        var user = new User { Email = "a@test" };
        user.Orders.Add(new Order { Description = "o" });
        scope.Db.Users.Add(user);

        await Save.Invoke(scope.Db, call);

        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        Assert.Equal(1, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_WithBufferedEvent_LeavesAutoTransactionBehaviorWhenNeeded(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        Assert.Equal(AutoTransactionBehavior.WhenNeeded, scope.Db.Database.AutoTransactionBehavior);

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        await Save.Invoke(scope.Db, async);

        Assert.Equal(1, host.Count("OutboxMessages")); // guard: the outbox path really ran
        Assert.Equal(AutoTransactionBehavior.WhenNeeded, scope.Db.Database.AutoTransactionBehavior);
    }
}
