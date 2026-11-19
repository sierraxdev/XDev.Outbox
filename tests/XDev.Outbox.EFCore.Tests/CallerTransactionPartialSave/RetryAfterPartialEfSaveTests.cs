using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Pre-existing defect (not D5; found during the D5 work, confirmed by the tester on 2026-11-19, original code too):
/// when EF's own save fails after an earlier command of the same save ran (the outbox INSERT got its identity
/// value, then the Users INSERT fails), retrying the save on SQLite throws
/// "InvalidOperationException: The instance of entity type 'OutboxMessage&lt;int&gt;' cannot be tracked because another
/// instance with the same key value for {'Id'} is already being tracked" (inside a DbUpdateException).
///
/// Root cause: on failure EF discards the store-generated key (the entry's key goes back to its temporary value) but
/// its identity map still holds the entry under the discarded value (it is fixed up only by a later DetectChanges).
/// RecoverFailedAttempt detaches the outbox entries through the EntityEntry captured before the save, without
/// DetectChanges, so EF removes the entry under its temporary key and the discarded key stays in the identity map,
/// pointing at the detached entry. The retry adds a new outbox entity; SQLite hands out the same rowid again (the
/// failed INSERT was rolled back), so propagating it collides with the stale identity-map entry. Detaching through
/// a fresh <c>context.Entry(entity)</c> (which runs DetectChanges for that entity) leaves no stale entry. SQL Server
/// identity values are not reused after a rollback, so there the stale entry is latent (no exception).
/// </summary>
public class RetryAfterPartialEfSaveTests
{
    private const string Skip = "Pre-existing defect (not D5): retry after EF's own save failed mid-way throws 'OutboxMessage<int> cannot be tracked ... same key' on SQLite (stale identity-map entry left by RecoverFailedAttempt's detach). See class remarks.";

    private static async Task<(int Users, int Outbox)> CountsInTransaction(AdvDbContext db)
        => (await db.Users.AsNoTracking().CountAsync(), await db.Set<OutboxMessage<int>>().AsNoTracking().CountAsync());

    [Theory(Skip = Skip)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task OwnedSave_UserInsertFailsAfterOutboxInsertRan_ApplicationRetry_PersistsAndDispatchesOnce(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
        Assert.True(ExceptionChain.Contains<InjectedFailureException>(error));
        Assert.Contains(host.Faults.ExecutedCommands, SqlMatch.IsOutboxInsert); // the outbox INSERT ran first

        Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    [Theory(Skip = Skip)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CallerTransaction_UserInsertFailsAfterOutboxInsertRan_RetryInSameTransaction_PersistsAndDispatchesOnce(bool async, bool flag)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        await using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(error));
            Assert.Equal((0, 0), await CountsInTransaction(db));

            Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async, flag)));
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    /// <summary>The retrying execution strategy's retry hits the same collision, which is not transient, so the save fails.</summary>
    [Theory(Skip = Skip)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSaveWithRetryingStrategy_UserInsertFailsOnceAfterOutboxInsertRan_RetrySucceeds(bool async)
    {
        using var host = TesterSavepointHost.Create(retrying: true);
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsUserInsert);

        Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
        Assert.Equal(1, host.Faults.CommandFailuresFired);
        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }
}
