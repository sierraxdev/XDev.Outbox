using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

public enum FailurePoint
{
    /// <summary>The INSERT of the outbox row fails.</summary>
    OutboxInsert,

    /// <summary>
    /// The payload fix-up UPDATE of the outbox row fails (D1). This statement only exists after the fix;
    /// against the pre-fix code no such command runs, so the injected failure never fires.
    /// </summary>
    PayloadUpdate,

    /// <summary>The commit of the library-owned transaction fails.</summary>
    Commit,
}

/// <summary>
/// Library-owned transaction failure handling (#2, D2): a failed save leaves the database, the change
/// tracker, the IOutbox buffer and the dispatcher as if the save had not been attempted, so the
/// application can retry the same save.
/// </summary>
public class OwnedTransactionFailureTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task OwnedSave_WhenOutboxInsertFails_RollsBackKeepsBufferAndAppRetrySucceedsOnce(bool async)
        => FailedOwnedSaveThenAppRetry(FailurePoint.OutboxInsert, async);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task OwnedSave_WhenCommitFails_RollsBackPreservesTrackerStateAndAppRetrySucceedsOnce(bool async)
        => FailedOwnedSaveThenAppRetry(FailurePoint.Commit, async);

    /// <summary>
    /// Only meaningful after the fix introduces the payload UPDATE (D1). Against the pre-fix code it fails at
    /// the "save must throw" assertion because no UPDATE of the outbox table is ever executed.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task OwnedSave_WhenPayloadUpdateFails_RollsBackKeepsBufferAndAppRetrySucceedsOnce(bool async)
        => FailedOwnedSaveThenAppRetry(FailurePoint.PayloadUpdate, async);

    [Theory]
    [InlineData(FailurePoint.OutboxInsert, false)]
    [InlineData(FailurePoint.OutboxInsert, true)]
    [InlineData(FailurePoint.Commit, false)]
    [InlineData(FailurePoint.Commit, true)]
    public async Task OwnedSave_AfterFailedSaveAndAbandonedEvent_UnrelatedSaveWritesNoPhantomOutboxRow(FailurePoint failure, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        Inject(host, failure);
        var ex = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));
        Assert.True(ex != null, $"Precondition: the save with an injected {failure} failure must throw.");
        host.Faults.Clear();

        // The application abandons the work: drops the event and stops tracking the user.
        scope.Outbox.ClearEvents();
        scope.Db.Entry(user).State = EntityState.Detached;

        // Unrelated multi-statement save (EF opens its own transaction, so the commit hook runs).
        scope.Db.VersionedItems.AddRange(new Versioned { Name = "x" }, new Versioned { Name = "y" });
        await Save.Invoke(scope.Db, async);

        Assert.Equal(2, host.Count("VersionedItems"));
        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Empty(host.Queue.Enqueued);
    }

    private static void Inject(OutboxTestHost host, FailurePoint failure)
    {
        switch (failure)
        {
            case FailurePoint.OutboxInsert:
                host.Faults.FailCommands(SqlMatch.IsOutboxInsert);
                break;
            case FailurePoint.PayloadUpdate:
                host.Faults.FailCommands(SqlMatch.IsOutboxUpdate);
                break;
            case FailurePoint.Commit:
                host.Faults.FailCommits();
                break;
        }
    }

    private static async Task FailedOwnedSaveThenAppRetry(FailurePoint failure, bool async)
    {
        using var host = OutboxTestHost.Create();
        var versionedId = host.SeedVersioned("a", 1);
        using var scope = host.CreateScope();

        // A modified entity with a concurrency token: losing its original values would make the retry fail.
        var versioned = scope.Db.VersionedItems.Single(x => x.Id == versionedId);
        versioned.Name = "b";
        versioned.Version = 2;

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        var evt = new UserCreated { User = user };
        scope.Outbox.AddEvent(evt);

        Inject(host, failure);

        var ex = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));

        Assert.True(ex != null, $"The save must throw when the {failure} step fails (no injected failure fired: the step was never executed).");
        Assert.True(ExceptionChain.Contains<InjectedFailureException>(ex), ex.ToString());

        // Database: nothing committed.
        Assert.Equal(0, host.Count("Users"));
        Assert.Equal(0, host.Count("OutboxMessages"));
        Assert.Equal(("a", 1), host.ReadVersioned(versionedId));

        // Change tracker: as before the save.
        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
        var versionedEntry = scope.Db.Entry(versioned);
        Assert.Equal(EntityState.Modified, versionedEntry.State);
        Assert.Equal("a", versionedEntry.Property(x => x.Name).OriginalValue);
        Assert.Equal(1, versionedEntry.Property(x => x.Version).OriginalValue);
        Assert.True(versionedEntry.Property(x => x.Name).IsModified);
        Assert.True(versionedEntry.Property(x => x.Version).IsModified);
        Assert.Equal(0, scope.TrackedOutboxMessages);

        // Buffer kept (D2), nothing dispatched.
        Assert.Same(evt, Assert.Single(scope.Outbox.GetEvents()));
        Assert.Empty(host.Queue.Enqueued);

        // Application-level retry of the same save succeeds exactly once.
        host.Faults.Clear();
        await Save.Invoke(scope.Db, async);

        Assert.Equal(("b", 2), host.ReadVersioned(versionedId));
        var userId = Assert.Single(host.UserIds());
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(userId, Payload.User(row.Payload).GetProperty("id").GetInt32());
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);
        Assert.Empty(scope.Outbox.GetEvents());
        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        Assert.Equal(userId, user.Id);
    }
}
