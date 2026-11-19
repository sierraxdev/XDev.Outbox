using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Round2;

/// <summary>Outbox whose messages are hidden by a global query filter (the savepoint existence check must ignore it).</summary>
public class FilteredOutboxDbContext : OutboxDbContext<int>
{
    public FilteredOutboxDbContext(DbContextOptions<FilteredOutboxDbContext> options) : base(options) { }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.Entity<User>(b => { b.ToTable("Users"); b.Ignore(x => x.Orders); });
        base.OnModelCreating(builder);
        builder.Entity<OutboxMessage<int>>().HasQueryFilter(m => m.Id < 0);
    }
}

/// <summary>
/// Final correction round: unconfirmed messages after a failed savepoint existence check (D-B), acceptance by a later
/// save without events (D-C), stale messages on commit (D-A) and the existence query shape (N6).
/// </summary>
public class FinalRoundTests
{
    public static TheoryData<bool> Async => new() { false, true };

    private static bool IsOutboxSelect(string sql)
        => sql.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) && sql.Contains("OutboxMessages");

    /// <summary>
    /// The existence check fails after the rollback to the savepoint and again just before commit. The commit is not
    /// blocked; no message is dispatched in memory (the committed row is left to the outbox poller); the buffer is empty.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task SavepointCheckFails_CommitCheckAlsoFails_CommitSucceeds_NothingDispatchedInMemory(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await R2Save.Begin(db, async))
        {
            db.Users.Add(new User { Email = "1@test" });
            outbox.AddEvent(new PlainEvent { Text = "kept" });
            await R2Save.Invoke(db, async, true);

            await R2Save.Savepoint(tx, "sp", async);
            db.Users.Add(new User { Email = "2@test" });
            outbox.AddEvent(new PlainEvent { Text = "rolled-back" });
            await R2Save.Invoke(db, async, true);

            host.Faults.FailCommands(IsOutboxSelect, times: 2);
            Assert.NotNull(await Record.ExceptionAsync(() => R2Save.RollbackTo(tx, "sp", async)));

            await R2Save.Commit(tx, async);
            host.Faults.Clear();
        }

        Assert.Equal(2, host.Faults.CommandFailuresFired);
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal("kept", R2Host.Root(row.Payload).GetProperty("text").GetString());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(outbox.GetEvents());
    }

    /// <summary>
    /// The existence check fails; the caller saves the not-accepted changes again in the same transaction. The save
    /// checks first, finds the row undone, and writes the event again with the business row: exactly one row and one
    /// dispatch, no loss.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task SavepointCheckFails_NotAcceptedChangesSavedAgain_EventWrittenOnceWithThem(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var evt = new PlainEvent { Text = "e" };
        using (var tx = await R2Save.Begin(db, async))
        {
            await R2Save.Savepoint(tx, "sp", async);
            db.Users.Add(new User { Email = "u@test" });
            outbox.AddEvent(evt);
            await R2Save.Invoke(db, async, false);

            host.Faults.FailCommands(IsOutboxSelect);
            Assert.NotNull(await Record.ExceptionAsync(() => R2Save.RollbackTo(tx, "sp", async)));
            host.Faults.Clear();

            await R2Save.Invoke(db, async, false);
            await R2Save.Commit(tx, async);
        }
        db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
        Assert.Empty(outbox.GetEvents());
    }

    /// <summary>The check made by the next save fails too: the save surfaces the error and writes nothing.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task SavepointCheckFails_NextSaveCheckFails_SaveThrowsAndWritesNothing(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await R2Save.Begin(db, async))
        {
            await R2Save.Savepoint(tx, "sp", async);
            db.Users.Add(new User { Email = "u@test" });
            outbox.AddEvent(new PlainEvent { Text = "e" });
            await R2Save.Invoke(db, async, false);

            host.Faults.FailCommands(IsOutboxSelect, times: 2);
            Assert.NotNull(await Record.ExceptionAsync(() => R2Save.RollbackTo(tx, "sp", async)));
            var commandsBefore = host.Faults.ExecutedCommands.Count;

            var saveError = await Record.ExceptionAsync(() => R2Save.Invoke(db, async, false));
            Assert.True(ExceptionChain.Contains<InjectedFailureException>(saveError!), saveError?.ToString());
            Assert.DoesNotContain(host.Faults.ExecutedCommands.Skip(commandsBefore), c => c.Contains("INSERT"));
            host.Faults.Clear();

            await R2Save.Commit(tx, async);
        }

        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// A flag=false save is followed in the same transaction by a flag=true save that has no events of its own; it
    /// accepts the earlier changes, so after a rollback the earlier event is not kept (it would be a phantom).
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task NotAcceptedSave_ThenAcceptingSaveWithoutEvents_CallerRollsBack_EventDropped(bool async)
    {
        using var host = R2Host.Create();
        host.Exec("INSERT INTO \"VersionedItems\" (\"Name\", \"Version\") VALUES ('a', 1)");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var a = db.VersionedItems.Single();
        var evt = new PlainEvent { Text = "A" };
        using (var tx = await R2Save.Begin(db, async))
        {
            a.Name = "a-x";
            outbox.AddEvent(evt);
            await R2Save.Invoke(db, async, false);

            db.Users.Add(new User { Email = "b@test" });
            await R2Save.Invoke(db, async, true);

            await R2Save.Rollback(tx, async);
        }

        Assert.Equal(EntityState.Unchanged, db.Entry(a).State);
        Assert.Empty(outbox.GetEvents());
        await R2Save.Invoke(db, async, true);
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// tx1 saves with flag=true and is disposed; tx2 commits without a save. tx1's event is dropped (its changes were
    /// accepted and will not be saved again) and nothing is dispatched.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task AcceptedSave_TransactionDisposed_ThenOtherTransactionCommitsWithoutSave_EventDroppedNotDispatched(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "u@test" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });

        using (await R2Save.Begin(db, async))
        {
            await R2Save.Invoke(db, async, true);
        }

        using (var tx2 = await R2Save.Begin(db, async))
        {
            await R2Save.Commit(tx2, async);
        }

        Assert.Empty(outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(host.OutboxRows());
    }

    /// <summary>Savepoint handling with a Guid outbox key (existence query equality on Guid).</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task GuidKey_RollbackToSavepoint_CommitDispatchesOnlySurvivor(bool async)
    {
        using var host = AdvHost<GuidOutboxDbContext, Guid>.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await R2Save.Begin(db, async))
        {
            db.Users.Add(new User { Email = "1@test" });
            outbox.AddEvent(new PlainEvent { Text = "kept" });
            await R2Save.Invoke(db, async, true);

            await R2Save.Savepoint(tx, "sp", async);
            var rolledBack = new User { Email = "2@test" };
            db.Users.Add(rolledBack);
            outbox.AddEvent(new PlainEvent { Text = "rolled-back" });
            await R2Save.Invoke(db, async, true);
            await R2Save.RollbackTo(tx, "sp", async);
            db.Entry(rolledBack).State = EntityState.Detached;

            await R2Save.Commit(tx, async);
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(Guid.Parse(row.Id), Assert.Single(host.Queue.Enqueued).Id);
    }

    /// <summary>A global query filter on the outbox entity does not make surviving rows look rolled back.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task QueryFilterOnOutbox_RollbackToSavepoint_SurvivorStillDispatched(bool async)
    {
        using var host = AdvHost<FilteredOutboxDbContext, int>.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await R2Save.Begin(db, async))
        {
            db.Users.Add(new User { Email = "1@test" });
            outbox.AddEvent(new PlainEvent { Text = "kept" });
            await R2Save.Invoke(db, async, true);

            await R2Save.Savepoint(tx, "sp", async);
            db.Users.Add(new User { Email = "2@test" });
            outbox.AddEvent(new PlainEvent { Text = "rolled-back" });
            await R2Save.Invoke(db, async, true);
            await R2Save.RollbackTo(tx, "sp", async);

            // The surviving message is still pending (its event stays buffered until the commit); the undone one is gone.
            Assert.Equal("kept", Assert.IsType<PlainEvent>(Assert.Single(outbox.GetEvents())).Text);
            await R2Save.Commit(tx, async);
        }

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(int.Parse(row.Id), Assert.Single(host.Queue.Enqueued).Id);
        Assert.Empty(outbox.GetEvents());
    }
}
