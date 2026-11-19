using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Round2;

/// <summary>
/// Correction round: B1 buffer rule (events follow the business changes they were saved with) and savepoint
/// handling inside caller-owned transactions.
/// </summary>
public class CallerTransactionRound2Tests
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

    private static int SeedVersioned(R2Host host, string name)
    {
        host.Exec($"INSERT INTO \"VersionedItems\" (\"Name\", \"Version\") VALUES ('{name}', 1)");
        return (int)host.Scalar("SELECT MAX(\"Id\") FROM \"VersionedItems\"");
    }

    // ---------------- B1 buffer rule ----------------

    /// <summary>Two flag=false saves in one transaction, rollback: both events stay buffered in their original order.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task TwoNotAcceptedSaves_CallerRollsBack_BufferKeepsBothEventsInOrder_RetryWritesBothInOrder(bool async)
    {
        using var host = R2Host.Create();
        var id1 = SeedVersioned(host, "one");
        var id2 = SeedVersioned(host, "two");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var e1 = new PlainEvent { Text = "e1" };
        var e2 = new PlainEvent { Text = "e2" };
        using (var tx = await R2Save.Begin(db, async))
        {
            db.VersionedItems.Single(x => x.Id == id1).Name = "one-x";
            outbox.AddEvent(e1);
            await R2Save.Invoke(db, async, false);

            db.VersionedItems.Single(x => x.Id == id2).Name = "two-x";
            outbox.AddEvent(e2);
            await R2Save.Invoke(db, async, false);

            await R2Save.Rollback(tx, async);
        }

        Assert.Equal(new object[] { e1, e2 }, outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);

        using (var tx = await R2Save.Begin(db, async))
        {
            await R2Save.Invoke(db, async, false);
            await R2Save.Commit(tx, async);
        }
        db.ChangeTracker.AcceptAllChanges();

        var rows = host.OutboxRows();
        Assert.Equal(new[] { "e1", "e2" }, rows.Select(r => R2Host.Root(r.Payload).GetProperty("text").GetString()));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id));
        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"VersionedItems\" WHERE \"Name\" = 'one-x'"));
        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"VersionedItems\" WHERE \"Name\" = 'two-x'"));
    }

    /// <summary>
    /// Mixed flags in one transaction: A saved with flag=false, then B saved with flag=true. B's save accepts ALL
    /// tracked changes (including A's), so after a rollback neither A's nor B's business changes will be saved again.
    /// Per the B1 rule ("events follow the state of their business changes") neither event should survive.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task MixedFlags_NotAcceptedThenAccepted_CallerRollsBack_LaterSaveWritesNoEventForAcceptedChanges(bool async)
    {
        using var host = R2Host.Create();
        var idA = SeedVersioned(host, "a");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var eA = new PlainEvent { Text = "A" };
        var eB = new PlainEvent { Text = "B" };
        var a = db.VersionedItems.Single(x => x.Id == idA);
        using (var tx = await R2Save.Begin(db, async))
        {
            a.Name = "a-x";
            outbox.AddEvent(eA);
            await R2Save.Invoke(db, async, false);

            db.Users.Add(new User { Email = "b@test" });
            outbox.AddEvent(eB);
            await R2Save.Invoke(db, async, true);

            await R2Save.Rollback(tx, async);
        }

        // Observed tracker state: A's change was accepted by B's save, so it is not pending any more.
        Assert.Equal(EntityState.Unchanged, db.Entry(a).State);
        Assert.Empty(host.Queue.Enqueued);
        Assert.DoesNotContain(eB, outbox.GetEvents());

        Assert.DoesNotContain(eA, outbox.GetEvents());
        await R2Save.Invoke(db, async, true);

        Assert.Equal("a", (string)host.Connection.CreateCommandScalar($"SELECT \"Name\" FROM \"VersionedItems\" WHERE \"Id\" = {idA}"));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// Same event instance saved twice in one transaction with different flags, rollback. The flag=true save accepted
    /// every tracked change, including the earlier flag=false save's change (D-C), so neither copy follows a pending
    /// change: both are dropped. Keeping one would produce a phantom message on the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task SameEventInstance_SavedWithFalseThenTrue_CallerRollsBack_BothCopiesDropped_NextSaveWritesNothing(bool async)
    {
        using var host = R2Host.Create();
        var id = SeedVersioned(host, "a");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var e = new PlainEvent { Text = "same" };
        using (var tx = await R2Save.Begin(db, async))
        {
            db.VersionedItems.Single(x => x.Id == id).Name = "a-x";
            outbox.AddEvent(e);
            await R2Save.Invoke(db, async, false);
            outbox.AddEvent(e);
            await R2Save.Invoke(db, async, true);
            Assert.Equal(2, host.Faults.ExecutedCommands.Count(c => c.Contains("INSERT INTO \"OutboxMessages\"")));
            await R2Save.Rollback(tx, async);
        }

        Assert.Empty(outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);

        // A subsequent save (no pending changes, change already accepted) must not resurrect the event.
        await R2Save.Invoke(db, async, true);

        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal("a", (string)host.Connection.CreateCommandScalar($"SELECT \"Name\" FROM \"VersionedItems\" WHERE \"Id\" = {id}"));
    }

    /// <summary>Same event instance added twice and saved once with flag=false, rollback: both copies stay buffered.</summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task SameEventInstanceTwice_NotAccepted_CallerRollsBack_BothCopiesStayBuffered(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var e = new PlainEvent { Text = "same" };
        db.Users.Add(new User { Email = "u@test" });
        outbox.AddEvent(e);
        outbox.AddEvent(e);
        using (var tx = await R2Save.Begin(db, async))
        {
            await R2Save.Invoke(db, async, false);
            await R2Save.Rollback(tx, async);
        }

        Assert.Equal(new object[] { e, e }, outbox.GetEvents());
    }

    /// <summary>
    /// Documents the asymmetry for events added after the save but never saved: rollback clears them, while a
    /// dispose without rollback keeps them (and the next save writes them).
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task NeverSavedEvent_RollbackDropsIt_DisposeKeepsIt(bool async, bool flag)
    {
        using var host = R2Host.Create();

        // Rollback.
        {
            var (scope, db, outbox) = host.CreateScope();
            using var _ = scope;
            var saved = new PlainEvent { Text = "saved" };
            var never = new PlainEvent { Text = "never" };
            db.Users.Add(new User { Email = "r@test" });
            outbox.AddEvent(saved);
            using (var tx = await R2Save.Begin(db, async))
            {
                await R2Save.Invoke(db, async, flag);
                outbox.AddEvent(never);
                await R2Save.Rollback(tx, async);
            }
            Assert.Equal(flag ? Array.Empty<object>() : new object[] { saved }, outbox.GetEvents());
        }

        // Dispose without commit/rollback; detection happens on the next save.
        {
            var (scope, db, outbox) = host.CreateScope();
            using var _ = scope;
            var saved = new PlainEvent { Text = "saved" };
            var never = new PlainEvent { Text = "never" };
            db.Users.Add(new User { Email = "d@test" });
            outbox.AddEvent(saved);
            using (await R2Save.Begin(db, async))
            {
                await R2Save.Invoke(db, async, flag);
                outbox.AddEvent(never);
            }
            await R2Save.Invoke(db, async, true);

            var texts = host.OutboxRows().Select(r => R2Host.Root(r.Payload).GetProperty("text").GetString()).ToList();
            Assert.Equal(flag ? new[] { "never" } : new[] { "saved", "never" }, texts);
            Assert.Equal(flag ? 0 : 1, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        }
    }

    /// <summary>
    /// tx1: save with flag=false, then disposed (e.g. exception before commit). tx2 commits without any SaveChanges
    /// (raw SQL only). The business change is still pending in the tracker, so its event must still be buffered and
    /// written by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task NotAcceptedSave_TransactionDisposed_ThenOtherTransactionCommitsWithoutSave_EventStillWrittenWithBusinessRow(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "u@test" };
        db.Users.Add(user);
        var evt = new UserCreated { User = user };
        outbox.AddEvent(evt);

        using (await R2Save.Begin(db, async))
        {
            await R2Save.Invoke(db, async, false);
        }

        using (var tx2 = await R2Save.Begin(db, async))
        {
            if (async) await db.Database.ExecuteSqlRawAsync("UPDATE \"VersionedItems\" SET \"Name\" = 'x' WHERE 0 = 1");
            else db.Database.ExecuteSqlRaw("UPDATE \"VersionedItems\" SET \"Name\" = 'x' WHERE 0 = 1");
            await R2Save.Commit(tx2, async);
        }

        Assert.Equal(EntityState.Added, db.Entry(user).State);
        Assert.Contains(evt, outbox.GetEvents());

        await R2Save.Invoke(db, async, true);
        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    // ---------------- Savepoints ----------------

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task NestedSavepoints_RollbackToInnerThenOuter_DropsBatchesIndependently(bool async, bool flag)
    {
        using var host = R2Host.Create();
        var id1 = SeedVersioned(host, "one");
        var id2 = SeedVersioned(host, "two");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var e1 = new PlainEvent { Text = "e1" };
        var e2 = new PlainEvent { Text = "e2" };
        using (var tx = await R2Save.Begin(db, async))
        {
            await R2Save.Savepoint(tx, "sp1", async);
            db.VersionedItems.Single(x => x.Id == id1).Name = "one-x";
            outbox.AddEvent(e1);
            await R2Save.Invoke(db, async, flag);

            await R2Save.Savepoint(tx, "sp2", async);
            db.VersionedItems.Single(x => x.Id == id2).Name = "two-x";
            outbox.AddEvent(e2);
            await R2Save.Invoke(db, async, flag);

            await R2Save.RollbackTo(tx, "sp2", async);
            // e1 still pending in the transaction; e2 follows the flag.
            Assert.Equal(flag ? new object[] { e1 } : new object[] { e1, e2 }, outbox.GetEvents());

            await R2Save.RollbackTo(tx, "sp1", async);
            Assert.Equal(flag ? Array.Empty<object>() : new object[] { e1, e2 }, outbox.GetEvents());

            await R2Save.Commit(tx, async);
        }

        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
    }

    [Theory]
    [MemberData(nameof(Async))]
    public async Task RollbackToSavepoint_ThenFurtherSavesAndCommit_DispatchesSurvivingMessagesInOrder(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await R2Save.Begin(db, async))
        {
            db.Users.Add(new User { Email = "1@test" });
            outbox.AddEvent(new PlainEvent { Text = "e1" });
            await R2Save.Invoke(db, async, true);

            await R2Save.Savepoint(tx, "sp", async);
            var rolledBackUser = new User { Email = "2@test" };
            db.Users.Add(rolledBackUser);
            outbox.AddEvent(new PlainEvent { Text = "e2" });
            await R2Save.Invoke(db, async, true);
            await R2Save.RollbackTo(tx, "sp", async);
            // Plain EF: the accepted entity's row was undone; the caller stops tracking it (its key will be reused).
            db.Entry(rolledBackUser).State = EntityState.Detached;

            db.Users.Add(new User { Email = "3@test" });
            outbox.AddEvent(new PlainEvent { Text = "e3" });
            await R2Save.Invoke(db, async, true);

            db.Users.Add(new User { Email = "4@test" });
            outbox.AddEvent(new PlainEvent { Text = "e4" });
            await R2Save.Invoke(db, async, true);

            await R2Save.Commit(tx, async);
        }

        var rows = host.OutboxRows();
        Assert.Equal(new[] { "e1", "e3", "e4" }, rows.Select(r => R2Host.Root(r.Payload).GetProperty("text").GetString()));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id));
        Assert.Equal(3, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(outbox.GetEvents());
    }

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task ReleaseSavepoint_ThenCommit_DispatchesEveryMessage(bool async, bool flag)
    {
        using var host = R2Host.Create();
        var id1 = SeedVersioned(host, "one");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = await R2Save.Begin(db, async))
        {
            outbox.AddEvent(new PlainEvent { Text = "e0" });
            await R2Save.Invoke(db, async, flag);
            await R2Save.Savepoint(tx, "sp", async);
            db.VersionedItems.Single(x => x.Id == id1).Name = "one-x";
            outbox.AddEvent(new PlainEvent { Text = "e1" });
            await R2Save.Invoke(db, async, flag);
            await R2Save.Release(tx, "sp", async);
            await R2Save.Commit(tx, async);
        }

        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(host.OutboxRows().Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id));
    }

    /// <summary>
    /// The existence query issued after a rollback to a savepoint fails. The exception surfaces from
    /// RollbackToSavepoint; if the caller swallows it and commits, the rolled-back message must not be dispatched.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task RollbackToSavepoint_ExistenceQueryFails_CallerCommits_RolledBackMessageNotDispatched(bool async)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        Exception? rollbackError;
        using (var tx = await R2Save.Begin(db, async))
        {
            db.Users.Add(new User { Email = "1@test" });
            outbox.AddEvent(new PlainEvent { Text = "kept" });
            await R2Save.Invoke(db, async, true);

            await R2Save.Savepoint(tx, "sp", async);
            db.Users.Add(new User { Email = "2@test" });
            outbox.AddEvent(new PlainEvent { Text = "rolled-back" });
            await R2Save.Invoke(db, async, true);

            host.Faults.FailCommands(sql => sql.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) && sql.Contains("OutboxMessages"));
            rollbackError = await Record.ExceptionAsync(() => R2Save.RollbackTo(tx, "sp", async));
            host.Faults.Clear();

            await R2Save.Commit(tx, async);
        }

        Assert.Equal(1, host.Faults.CommandFailuresFired);
        Assert.NotNull(rollbackError);
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal("kept", R2Host.Root(row.Payload).GetProperty("text").GetString());
        // No phantom: every dispatched message has a committed row.
        Assert.All(host.Queue.Enqueued, m => Assert.Equal(row.Id, m.Id));
        Assert.Single(host.Queue.Enqueued);
    }

    /// <summary>More than 100 pending messages (existence query batching) with a partial rollback to a savepoint.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task ManyPendingMessages_PartialRollbackToSavepoint_CommitDispatchesExactlyTheSurvivors(bool async, bool flag)
    {
        using var host = R2Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var rolledBack = Enumerable.Range(0, 90).Select(i => new PlainEvent { Text = "rb" + i }).ToList();
        using (var tx = await R2Save.Begin(db, async))
        {
            foreach (var i in Enumerable.Range(0, 130)) outbox.AddEvent(new PlainEvent { Text = "k" + i });
            await R2Save.Invoke(db, async, flag);

            await R2Save.Savepoint(tx, "sp", async);
            foreach (var e in rolledBack) outbox.AddEvent(e);
            await R2Save.Invoke(db, async, flag);

            var commandsBefore = host.Faults.ExecutedCommands.Count;
            await R2Save.RollbackTo(tx, "sp", async);
            var selects = host.Faults.ExecutedCommands.Skip(commandsBefore).Count(c => c.Contains("SELECT") && c.Contains("OutboxMessages"));
            Assert.Equal(3, selects); // 220 pending ids, batches of 100

            Assert.Equal(130 + (flag ? 0 : 90), outbox.GetEvents().Count);
            await R2Save.Commit(tx, async);
        }

        var rows = host.OutboxRows();
        Assert.Equal(130, rows.Count);
        Assert.All(rows, r => Assert.StartsWith("k", R2Host.Root(r.Payload).GetProperty("text").GetString()));
        Assert.Equal(rows.Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id));
    }
}

internal static class ConnectionExtensions
{
    public static object CreateCommandScalar(this Microsoft.Data.Sqlite.SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar()!;
    }
}
