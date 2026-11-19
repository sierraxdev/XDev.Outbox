using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Round2;

/// <summary>
/// Setter fallback (properties with no backing field EF can discover): the projection and restore call the property
/// setter. A domain guard in the setter (reject non-positive ids) must not leave entities projected, must surface
/// the error, and must not make the entity unsaveable when plain EF saves it fine.
/// </summary>
public class SetterFallbackRound2Tests
{
    public static TheoryData<bool> Async => new() { false, true };

    public static TheoryData<bool, bool> AsyncFlag()
    {
        var data = new TheoryData<bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(async, flag);
        return data;
    }

    private static bool IsSetterRejection(Exception ex)
        => ExceptionChain.Contains<ArgumentOutOfRangeException>(ex)
           || (ex is AggregateException agg && agg.InnerExceptions.Any(i => ExceptionChain.Contains<ArgumentOutOfRangeException>(i)));

    /// <summary>Baseline: plain EF (no event) saves an entity whose key setter rejects non-positive values.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task GuardedKeySetter_NoEvent_PlainEfSaveSucceeds(bool async, bool flag)
    {
        using var host = R2Host.Create();
        var (scope, db, _) = host.CreateScope();
        using var s = scope;

        var item = new GuardedItem { Name = "g", RejectNonPositive = true };
        db.GuardedItems.Add(item);
        await R2Save.Invoke(db, async, flag);
        if (!flag) db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"GuardedItems\""));
        Assert.Equal(1, item.Id);
    }

    /// <summary>
    /// Same entity with an event referencing it, flag=true. A successful flag=true save does not restore setter-only
    /// projected properties (AcceptAllChanges writes the store value anyway, matching plain EF's setter calls), so the
    /// guarded setter never sees a non-positive value and the save succeeds like plain EF.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task GuardedKeySetter_WithEvent_FlagTrue_SaveSucceedsLikePlainEf(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var s = scope;

        var item = new GuardedItem { Name = "g", RejectNonPositive = true };
        db.GuardedItems.Add(item);
        outbox.AddEvent(new GuardedCreated { Item = item });

        var ex = await Record.ExceptionAsync(() => R2Save.Invoke(db, async, true));
        Assert.Null(ex);

        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"GuardedItems\""));
        Assert.Equal(1, item.Id);
        Assert.Equal(1, R2Host.Root(Assert.Single(host.OutboxRows()).Payload).GetProperty("item").GetProperty("id").GetInt32());
        Assert.Single(host.Queue.Enqueued);
        Assert.Empty(outbox.GetEvents());
    }

    /// <summary>
    /// Documented limitation (D-D): with acceptAllChangesOnSuccess=false the setter-only projected key must be restored
    /// to its pre-save value (0) through the setter, and a guard rejecting non-positive values makes the save fail. The
    /// failure must be clean: nothing persisted, no outbox entity left tracked, the event still buffered, nothing
    /// enqueued.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task GuardedKeySetter_WithEvent_FlagFalse_DocumentedLimitation_SaveFailsCleanly(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var s = scope;

        var item = new GuardedItem { Name = "g", RejectNonPositive = true };
        var evt = new GuardedCreated { Item = item };
        db.GuardedItems.Add(item);
        outbox.AddEvent(evt);

        var ex = await Record.ExceptionAsync(() => R2Save.Invoke(db, async, false));

        Assert.NotNull(ex);
        Assert.True(IsSetterRejection(ex), ex.ToString());
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"GuardedItems\""));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
        Assert.Same(evt, Assert.Single(outbox.GetEvents()));
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// flag=false: the setter rejects the restore exactly once. The save fails with that error, nothing is persisted,
    /// every entity (field-backed and setter-backed) is back to its pre-save CLR value, the tracker still holds
    /// temporary keys, and an application retry persists everything with the real keys.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task GuardedKeySetter_FlagFalse_RestoreThrowsOnce_ErrorSurfacesStateRestoredAndAppRetrySucceeds(bool async)
    {
        using var host = R2Host.Create();
        host.Exec("INSERT INTO \"Users\" (\"Email\") VALUES ('pad1'), ('pad2')");
        var (scope, db, outbox) = host.CreateScope();
        using var s = scope;

        var user = new User { Email = "u@test" };
        var item = new GuardedItem { Name = "g", RejectNonPositiveRemaining = 1 };
        db.Users.Add(user);
        db.GuardedItems.Add(item);
        outbox.AddEvent(new GuardedCreated { Item = item, User = user });

        var ex = await Record.ExceptionAsync(() => R2Save.Invoke(db, async, false));

        Assert.NotNull(ex);
        Assert.True(IsSetterRejection(ex), ex.ToString());
        AssertFailedSaveLeftNothingAndRestoredState(host, db, outbox, user, item);

        await R2Save.Invoke(db, async, false);
        db.ChangeTracker.AcceptAllChanges();

        AssertRetryPersistedWithRealKeys(host, user, item);
    }

    /// <summary>
    /// flag=true: a successful save no longer restores setter-only properties (D-D), so the failure is injected on a
    /// path that still restores: the owned transaction's commit fails. The recovery restore writes the pre-save value
    /// through the guarded setter, which rejects it exactly once. The error surfaces, nothing is persisted, CLR and
    /// tracker state are back to pre-save, and an application retry persists everything with the real keys.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task GuardedKeySetter_FlagTrue_CommitFailsAndRestoreThrowsOnce_ErrorSurfacesStateRestoredAndAppRetrySucceeds(bool async)
    {
        using var host = R2Host.Create();
        host.Exec("INSERT INTO \"Users\" (\"Email\") VALUES ('pad1'), ('pad2')");
        var (scope, db, outbox) = host.CreateScope();
        using var s = scope;

        var user = new User { Email = "u@test" };
        var item = new GuardedItem { Name = "g", RejectNonPositiveRemaining = 1 };
        db.Users.Add(user);
        db.GuardedItems.Add(item);
        outbox.AddEvent(new GuardedCreated { Item = item, User = user });

        host.Faults.FailCommits(times: 1);
        var ex = await Record.ExceptionAsync(() => R2Save.Invoke(db, async, true));

        Assert.NotNull(ex);
        Assert.Equal(1, host.Faults.CommitFailuresFired);
        // The recovery restore went through the guarded setter and was rejected (the one-shot guard was consumed).
        Assert.Equal(0, item.RejectNonPositiveRemaining);
        // Both errors surface: the original commit failure and the setter's rejection during recovery.
        var agg = Assert.IsType<AggregateException>(ex);
        Assert.Contains(agg.InnerExceptions, i => i is InjectedFailureException);
        Assert.True(IsSetterRejection(ex), ex.ToString());
        AssertFailedSaveLeftNothingAndRestoredState(host, db, outbox, user, item);

        await R2Save.Invoke(db, async, true);

        AssertRetryPersistedWithRealKeys(host, user, item);
        Assert.Empty(outbox.GetEvents());
    }

    private static void AssertFailedSaveLeftNothingAndRestoredState(R2Host host, DbContext db, IOutbox outbox, User user, GuardedItem item)
    {
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"GuardedItems\""));
        Assert.Equal(2, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Single(outbox.GetEvents());
        Assert.Equal(0, user.Id);
        Assert.Equal(0, item.Id);
        Assert.Equal(EntityState.Added, db.Entry(user).State);
        Assert.Equal(EntityState.Added, db.Entry(item).State);
        Assert.True(db.Entry(user).Property(x => x.Id).IsTemporary);
        Assert.True(db.Entry(item).Property(x => x.Id).IsTemporary);
        Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
    }

    private static void AssertRetryPersistedWithRealKeys(R2Host host, User user, GuardedItem item)
    {
        Assert.Equal(3, user.Id);
        Assert.Equal(1, item.Id);
        var payload = R2Host.Root(Assert.Single(host.OutboxRows()).Payload);
        Assert.Equal(3, payload.GetProperty("user").GetProperty("id").GetInt32());
        Assert.Equal(1, payload.GetProperty("item").GetProperty("id").GetInt32());
        Assert.Single(host.Queue.Enqueued);
        Assert.Empty(host.InsertsWithExplicitId("Users", "GuardedItems"));
    }
}
