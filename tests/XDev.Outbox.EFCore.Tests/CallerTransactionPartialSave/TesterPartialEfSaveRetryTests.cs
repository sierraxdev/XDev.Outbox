#pragma warning disable EF1001 // IStateManager: used only to observe the identity map.
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Adversarial (tester) checks of fix-rollback-event-loss-and-retry-identity-conflict, bug 2: EF's own save fails
/// after some of its commands ran, at different command positions, with several outbox entries and users with
/// dependents, on the owned, caller and retrying-strategy paths, with AutoDetectChangesEnabled on and off. Also
/// checks that the recovery's per-entry DetectChanges does not detect (or save) unrelated business modifications
/// when the application disabled automatic change detection.
/// </summary>
public class TesterPartialEfSaveRetryTests
{
    private const int UserCount = 5;

    public enum SavePath
    {
        Owned,
        Caller,
        OwnedRetryingStrategy,
    }

    /// <summary>Which command runs and then fails (SQLite sends one INSERT per row: outbox rows first, then Users, then Orders).</summary>
    public enum FailAt
    {
        FirstOutboxInsert,
        MiddleOutboxInsert,
        LastOutboxInsert,
        FirstUserInsert,
        LastUserInsert,
        LastOrderInsert,
    }

    public static TheoryData<SavePath, FailAt, bool, bool> Matrix()
    {
        var data = new TheoryData<SavePath, FailAt, bool, bool>();
        foreach (var path in Enum.GetValues<SavePath>())
        foreach (var failAt in Enum.GetValues<FailAt>())
        foreach (var async in new[] { false, true })
        foreach (var autoDetect in new[] { true, false })
        {
            // The retrying strategy reruns the save itself: without automatic detection the application cannot call
            // DetectChanges between attempts, which plain EF needs as well when business keys were discarded.
            if (path == SavePath.OwnedRetryingStrategy && !autoDetect) continue;
            data.Add(path, failAt, async, autoDetect);
        }
        return data;
    }

    private static Func<string, bool> NthMatch(Func<string, bool> match, int n)
    {
        var seen = 0;
        return sql => match(sql) && ++seen == n;
    }

    private static bool IsOrderInsert(string sql) => sql.Contains("INSERT INTO \"Orders\"", StringComparison.Ordinal);

    private static Func<string, bool> Predicate(FailAt failAt) => failAt switch
    {
        FailAt.FirstOutboxInsert => NthMatch(SqlMatch.IsOutboxInsert, 1),
        FailAt.MiddleOutboxInsert => NthMatch(SqlMatch.IsOutboxInsert, (UserCount + 1) / 2),
        FailAt.LastOutboxInsert => NthMatch(SqlMatch.IsOutboxInsert, UserCount),
        FailAt.FirstUserInsert => NthMatch(SqlMatch.IsUserInsert, 1),
        FailAt.LastUserInsert => NthMatch(SqlMatch.IsUserInsert, UserCount),
        FailAt.LastOrderInsert => NthMatch(IsOrderInsert, UserCount),
        _ => throw new ArgumentOutOfRangeException(nameof(failAt)),
    };

    private static void AssertNoStaleOutboxIdentityEntry(DbContext db)
    {
        var stateManager = db.GetService<IStateManager>();
        var key = db.Model.FindEntityType(typeof(OutboxMessage<int>))!.FindPrimaryKey()!;
        Assert.All(Enumerable.Range(1, 100), id => Assert.Null(stateManager.TryGetEntry(key, new object[] { id })));
        Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
    }

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task EfSaveFailsAfterCommandRan_RetryPersistsEveryUserOrderAndEventOnceWithFinalKeys(SavePath path, FailAt failAt, bool async, bool autoDetect)
    {
        using var host = TesterSavepointHost.Create(retrying: path == SavePath.OwnedRetryingStrategy);
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;
        db.ChangeTracker.AutoDetectChangesEnabled = autoDetect;
        var users = Enumerable.Range(1, UserCount).Select(i => new User { Email = "u" + i, Orders = { new Order { Description = "o" + i } } }).ToList();
        foreach (var user in users)
        {
            db.Users.Add(user);
            outbox.AddEvent(new UserCreated { User = user });
        }
        host.Probe.FailAfterExecution(Predicate(failAt));

        var tx = path == SavePath.Caller ? (async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction()) : null;
        try
        {
            var error = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            if (path == SavePath.OwnedRetryingStrategy)
            {
                Assert.Null(error); // retried by the strategy
            }
            else
            {
                Assert.True(ExceptionChain.Contains<InjectedFailureException>(error), error?.ToString());
                Assert.Empty(host.Queue.Enqueued);
                AssertNoStaleOutboxIdentityEntry(db);
                Assert.All(users, u => Assert.Equal(EntityState.Added, db.Entry(u).State));
                if (!autoDetect) db.ChangeTracker.DetectChanges();
                Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            }
            Assert.Equal(1, host.Probe.AfterExecutionFailuresFired);
            if (tx != null)
            {
                if (async) await tx.CommitAsync(); else tx.Commit();
            }
        }
        finally
        {
            tx?.Dispose();
        }

        var dbUsers = host.Users();
        var rows = host.OutboxRows();
        Assert.Equal(users.Select(u => u.Email).OrderBy(x => x), dbUsers.Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(users.Select(u => (u.Id, u.Email)).OrderBy(x => x), dbUsers.OrderBy(x => x));
        Assert.Equal(UserCount, rows.Count);
        Assert.Equal(
            dbUsers.OrderBy(u => u.Email),
            rows.Select(r => Payload.User(r.Payload)).Select(p => (p.GetProperty("id").GetInt32(), p.GetProperty("email").GetString()!)).OrderBy(x => x.Item2));
        Assert.Equal(UserCount, host.Scalar("SELECT COUNT(*) FROM \"Orders\""));
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Orders\" o WHERE NOT EXISTS (SELECT 1 FROM \"Users\" u WHERE u.\"Id\" = o.\"UserId\")"));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id).OrderBy(x => x));
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// AutoDetectChangesEnabled = false and an unsaved, undetected modification of another tracked user. The recovery
    /// after a partial EF save detects changes only on its outbox entries: the other user stays Unchanged, the retry
    /// (no DetectChanges by the app) does not write its modification (plain EF parity), and a later explicit
    /// DetectChanges + save does.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task AutoDetectOff_UndetectedModificationOfOtherEntity_RecoveryDoesNotDetectOrSaveIt(bool async, bool callerTransaction)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        var existing = new User { Email = "existing" };
        db.Users.Add(existing);
        await Save.Invoke(db, async);
        db.ChangeTracker.AutoDetectChangesEnabled = false;

        existing.Email = "modified";
        var user = new User { Email = "new" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Probe.FailAfterExecution(SqlMatch.IsUserInsert); // the outbox INSERT ran first, then the user INSERT ran and fails

        await using (var tx = callerTransaction ? (async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction()) : null)
        {
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async))));
            Assert.Equal(EntityState.Unchanged, db.ChangeTracker.Entries<User>().Single(e => ReferenceEquals(e.Entity, existing)).State);
            AssertNoStaleOutboxIdentityEntry(db);

            // The new user's key was not discarded (its INSERT is the failing command), so no DetectChanges is needed.
            Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
            if (tx != null)
            {
                if (async) await tx.CommitAsync(); else tx.Commit();
            }
        }

        Assert.Equal(new[] { "existing", "new" }, host.Users().Select(u => u.Email).OrderBy(x => x));
        Assert.Equal(EntityState.Unchanged, db.ChangeTracker.Entries<User>().Single(e => ReferenceEquals(e.Entity, existing)).State);
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);

        db.ChangeTracker.DetectChanges();
        Assert.Equal(EntityState.Modified, db.Entry(existing).State);
        await Save.Invoke(db, async);
        Assert.Equal(new[] { "modified", "new" }, host.Users().Select(u => u.Email).OrderBy(x => x));
        Assert.Single(host.OutboxRows());
    }

    /// <summary>
    /// The application tracks an OutboxMessage itself (queried, Unchanged): the recovery after a partial EF save
    /// detaches only the attempt's own outbox entries, never the application's.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ApplicationTrackedOutboxRow_PartialEfSaveFails_OnlyAttemptEntriesDetached(bool async)
    {
        using var host = TesterSavepointHost.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _s = scope;

        outbox.AddEvent(new PlainEvent { Text = "first" });
        await Save.Invoke(db, async);
        var tracked = await db.Set<OutboxMessage<int>>().SingleAsync();
        Assert.Equal(EntityState.Unchanged, db.Entry(tracked).State);

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Probe.FailAfterExecution(SqlMatch.IsUserInsert);
        Assert.True(ExceptionChain.Contains<InjectedFailureException>(await Record.ExceptionAsync(() => Save.Invoke(db, async))));

        var outboxEntries = db.ChangeTracker.Entries<OutboxMessage<int>>().ToList();
        var only = Assert.Single(outboxEntries);
        Assert.Same(tracked, only.Entity);
        Assert.Equal(EntityState.Unchanged, only.State);

        Assert.Null(await Record.ExceptionAsync(() => Save.Invoke(db, async)));
        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(2, host.Queue.Enqueued.Count);
    }
}
