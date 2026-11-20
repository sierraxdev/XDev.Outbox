using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// Tester (adversarial) cases for fix-accept-after-owned-commit (Bug A): when ChangeTracker.AcceptAllChanges throws
/// after a library-owned save committed, the original exception propagates, the committed events leave the buffer
/// (never written again), events not saved by that save stay buffered and are written once later, and nothing is
/// enqueued in memory for that save (the poller delivers the committed rows after LockExpiresAt).
/// </summary>
public class TesterAcceptAfterOwnedCommitTests
{
    public static TheoryData<bool> Async() => new() { false, true };

    public static TheoryData<bool, string> AsyncEntity()
    {
        var data = new TheoryData<bool, string>();
        foreach (var async in new[] { false, true })
        foreach (var entity in new[] { nameof(User), nameof(Order), nameof(Customer), nameof(Address) })
            data.Add(async, entity);
        return data;
    }

    public sealed class AcceptanceFailure : Exception
    {
        public AcceptanceFailure(string entity) : base("acceptance failure on " + entity)
        {
        }
    }

    /// <summary>Throws (once) from ChangeTracker.StateChanged when an entry of the given CLR type name becomes Unchanged.</summary>
    private static Func<int> ThrowOnceWhenAccepted(DbContext db, string entityTypeName, Action<EntityStateChangedEventArgs>? before = null)
    {
        var thrown = 0;
        db.ChangeTracker.StateChanged += (_, e) =>
        {
            if (thrown == 0 && e.NewState == EntityState.Unchanged && e.Entry.Entity.GetType().Name == entityTypeName)
            {
                thrown++;
                before?.Invoke(e);
                throw new AcceptanceFailure(entityTypeName);
            }
        };
        return () => thrown;
    }

    private static List<(int Id, int UserId, string Description)> Orders(CommitTesterHost host)
    {
        using var command = host.Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"UserId\", \"Description\" FROM \"Orders\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, int, string)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetInt32(1), reader.GetString(2)));
        return rows;
    }

    private static List<(int Id, string Name, string? City)> Customers(CommitTesterHost host)
    {
        using var command = host.Connection.CreateCommand();
        command.CommandText = "SELECT \"Id\", \"Name\", \"Address_City\" FROM \"Customers\" ORDER BY \"Id\"";
        using var reader = command.ExecuteReader();
        var rows = new List<(int, string, string?)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        return rows;
    }

    private static int RowsWithText(CommitTesterHost host, string text) => host.OutboxRows().Count(r => r.Payload.Contains(text));

    private static void AssertNoPhantomOrDoubleDispatch(CommitTesterHost host)
    {
        var rowIds = host.OutboxRows().Select(r => r.Id).ToHashSet();
        var enqueued = host.Queue.Enqueued.Select(m => m.Id).ToList();
        Assert.All(enqueued, id => Assert.Contains(id, rowIds));
        Assert.Equal(enqueued.Count, enqueued.Distinct().Count());
    }

    private static async Task AssertNothingMoreIsWritten(CommitTesterHost host, AdvDbContext db, bool async)
    {
        var rows = host.OutboxRows().Count;
        var dispatched = host.Queue.Enqueued.Count;
        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "unrelated" });
        await Save.Invoke(db, async);
        Assert.Equal(rows, host.OutboxRows().Count);
        Assert.Equal(dispatched, host.Queue.Enqueued.Count);
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// Acceptance throws on different kinds of entries (principal, dependent, entity with an owned type, the owned type
    /// itself): the original exception propagates; the committed rows carry the store-generated keys even though
    /// acceptance did not complete; the buffer is empty, nothing is enqueued, and nothing is written again later.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncEntity))]
    public async Task OwnedSave_AcceptanceThrowsOnEntityKind_ExceptionPropagates_RowsCommittedWithKeys_NotRewritten(bool async, string entity)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var thrown = ThrowOnceWhenAccepted(db, entity);
        var user = new User { Email = "u@x", Orders = { new Order { Description = "o1" }, new Order { Description = "o2" } } };
        db.Users.Add(user);
        var customer = new Customer { Name = "c", Address = new Address { City = "Rome" } };
        db.Customers.Add(customer);
        db.Outbox.AddEvent(new UserCreated { User = user }, new CustomerCreated { Customer = customer });

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        var failure = Assert.IsType<AcceptanceFailure>(error);
        Assert.Contains(entity, failure.Message);
        Assert.Equal(1, thrown());
        Assert.Empty(db.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);

        var users = host.Users();
        var orders = Orders(host);
        var customers = Customers(host);
        var dbUser = Assert.Single(users);
        Assert.Equal(2, orders.Count);
        var dbCustomer = Assert.Single(customers);
        Assert.Equal("Rome", dbCustomer.City);

        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        var userPayload = JsonDocument.Parse(Assert.Single(rows, r => r.Payload.Contains("\"user\"")).Payload).RootElement.GetProperty("user");
        Assert.Equal(dbUser.Id, userPayload.GetProperty("id").GetInt32());
        var payloadOrders = userPayload.GetProperty("orders").EnumerateArray().ToList();
        Assert.Equal(2, payloadOrders.Count);
        foreach (var order in payloadOrders)
        {
            var dbOrder = Assert.Single(orders, o => o.Description == order.GetProperty("description").GetString());
            Assert.Equal(dbOrder.Id, order.GetProperty("id").GetInt32());
            Assert.Equal(dbUser.Id, order.GetProperty("userId").GetInt32());
        }
        var customerPayload = JsonDocument.Parse(Assert.Single(rows, r => r.Payload.Contains("\"customer\"")).Payload).RootElement.GetProperty("customer");
        Assert.Equal(dbCustomer.Id, customerPayload.GetProperty("id").GetInt32());
        Assert.Equal("Rome", customerPayload.GetProperty("address").GetProperty("city").GetString());

        db.ChangeTracker.AcceptAllChanges();
        await AssertNothingMoreIsWritten(host, db, async);
        Assert.Single(host.Users());
        Assert.Equal(2, Orders(host).Count);
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// The buffer is already trimmed when acceptance runs: a StateChanged handler that reads the buffer during the
    /// acceptance of a successful owned save sees only the event added during the save, not the committed one. The
    /// success path still dispatches the committed event once and keeps the during-save event for the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task OwnedSave_BufferObservedFromStateChangedDuringAcceptance_CommittedEventAlreadyRemoved(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var during = new PlainEvent { Text = "during save" };
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(during); };
        List<object>? observed = null;
        db.ChangeTracker.StateChanged += (_, e) =>
        {
            if (observed == null && e.Entry.Entity is User && e.NewState == EntityState.Unchanged)
            {
                observed = db.Outbox.GetEvents().ToList();
            }
        };
        var user = new User { Email = "a" };
        db.Users.Add(user);
        var saved = new UserCreated { User = user };
        db.Outbox.AddEvent(saved);

        await Save.Invoke(db, async, true);

        Assert.NotNull(observed);
        Assert.Same(during, Assert.Single(observed!));
        Assert.Single(host.Queue.Enqueued);
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "next" });
        await Save.Invoke(db, async);

        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Equal(2, host.Queue.Enqueued.Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// The throwing StateChanged handler adds an event before it throws (during acceptance): that event was never part
    /// of the save; it stays buffered and the next save writes and dispatches it once. The committed event is not
    /// rewritten and not dispatched in memory.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task OwnedSave_HandlerAddsEventThenThrowsDuringAcceptance_AddedEventWrittenOnceLater(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = new PlainEvent { Text = "added during acceptance" };
        ThrowOnceWhenAccepted(db, nameof(User), _ => db.Outbox.AddEvent(added));
        var user = new User { Email = "a" };
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        Assert.IsType<AcceptanceFailure>(error);
        Assert.Same(added, Assert.Single(db.Outbox.GetEvents()));
        Assert.Single(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "next" });
        await Save.Invoke(db, async);

        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "added during acceptance"));
        Assert.Equal(2, host.OutboxRows().Count);
        var dispatched = Assert.Single(host.Queue.Enqueued);
        Assert.Same(added, dispatched.Payload);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// Success path with an event added during acceptance (no throw): the committed event is dispatched once, the added
    /// event stays buffered and is written once by the next save.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task OwnedSave_HandlerAddsEventDuringAcceptance_NoThrow_CommittedDispatchedOnce_AddedWrittenOnceLater(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = new PlainEvent { Text = "added during acceptance" };
        var once = false;
        db.ChangeTracker.StateChanged += (_, e) =>
        {
            if (!once && e.Entry.Entity is User && e.NewState == EntityState.Unchanged)
            {
                once = true;
                db.Outbox.AddEvent(added);
            }
        };
        var user = new User { Email = "a" };
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        await Save.Invoke(db, async, true);

        Assert.Single(host.Queue.Enqueued);
        Assert.Same(added, Assert.Single(db.Outbox.GetEvents()));

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "next" });
        await Save.Invoke(db, async);

        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "added during acceptance"));
        Assert.Equal(2, host.Queue.Enqueued.Count);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// flag = false: the save does not run acceptance, so a StateChanged handler that throws on acceptance does not
    /// affect it (committed, dispatched once, buffer empty, payload carries the generated key). The application's own
    /// AcceptAllChanges then throws; nothing is written or dispatched again afterwards.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task OwnedSaveFlagFalse_ThrowingAcceptanceHandler_SaveUnaffected_AppAcceptanceThrowsLater_NoRewrite(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnceWhenAccepted(db, nameof(User));
        var user = new User { Email = "a" };
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        await Save.Invoke(db, async, false);

        Assert.Single(host.Queue.Enqueued);
        Assert.Empty(db.Outbox.GetEvents());
        var dbUser = Assert.Single(host.Users());
        Assert.Contains($"\"id\":{dbUser.Id}", Assert.Single(host.UserRows()).Payload);

        Assert.Throws<AcceptanceFailure>(() => db.ChangeTracker.AcceptAllChanges());
        db.ChangeTracker.AcceptAllChanges();

        await AssertNothingMoreIsWritten(host, db, async);
        Assert.Single(host.UserRows());
        Assert.Single(host.Queue.Enqueued);
        AssertNoPhantomOrDoubleDispatch(host);
    }

    /// <summary>
    /// The application retries the same unit of work after the acceptance failure without re-adding the event (two
    /// users: the second one is still Added with its store key, so plain EF re-inserts it and fails or duplicates it,
    /// which is EF behavior out of the library's scope). Whatever the business outcome, the library writes no further outbox row and dispatches
    /// nothing.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task OwnedSave_AcceptanceThrows_AppRetriesWithoutReAdding_NoFurtherOutboxRowOrDispatch(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnceWhenAccepted(db, nameof(User));
        var first = new User { Email = "a" };
        var second = new User { Email = "b" };
        db.Users.AddRange(first, second);
        db.Outbox.AddEvent(new UserCreated { User = first });

        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));
        Assert.IsType<AcceptanceFailure>(error);
        Assert.Contains(db.ChangeTracker.Entries<User>(), e => e.State == EntityState.Added);

        var retryError = await Record.ExceptionAsync(() => Save.Invoke(db, async, true));

        // Business outcome is plain EF (observed on SQLite: the still-Added user is inserted again, 3 user rows).
        _ = retryError;
        Assert.Single(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Empty(db.Outbox.GetEvents());
    }

    /// <summary>
    /// BY DESIGN: the application retries the unit of work and re-adds the same event instance: re-adding creates a
    /// second row (caused by the re-add, not by the library), written and dispatched once; the first committed row is
    /// never dispatched in memory (left to the poller). Both rows carry the same generated key.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task OwnedSave_AcceptanceThrows_AppReAddsSameEvent_SecondRowFromReAddOnly(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnceWhenAccepted(db, nameof(User));
        var user = new User { Email = "a" };
        db.Users.Add(user);
        var evt = new UserCreated { User = user };
        db.Outbox.AddEvent(evt);

        Assert.IsType<AcceptanceFailure>(await Record.ExceptionAsync(() => Save.Invoke(db, async, true)));
        Assert.Empty(db.Outbox.GetEvents());

        db.Outbox.AddEvent(evt);
        await Save.Invoke(db, async, true);

        var rows = host.UserRows();
        Assert.Equal(2, rows.Count);
        var dbUser = Assert.Single(host.Users());
        Assert.All(rows, r => Assert.Contains($"\"id\":{dbUser.Id}", r.Payload));
        var dispatched = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(rows[1].Id, dispatched.Id);
        AssertNoPhantomOrDoubleDispatch(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// Acceptance throws, then the application clears the buffer: the event added during the save is discarded (never
    /// written), the committed event is not written again, nothing is dispatched.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task OwnedSave_AcceptanceThrows_ThenClearEvents_NothingMoreWrittenOrDispatched(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnceWhenAccepted(db, nameof(User));
        var calls = 0;
        db.SavingChanges += (_, _) => { if (calls++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" }); };
        var user = new User { Email = "a" };
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        Assert.IsType<AcceptanceFailure>(await Record.ExceptionAsync(() => Save.Invoke(db, async, true)));
        Assert.Single(db.Outbox.GetEvents());
        db.Outbox.ClearEvents();

        await AssertNothingMoreIsWritten(host, db, async);
        Assert.Single(host.OutboxRows());
        Assert.Equal(0, RowsWithText(host, "during save"));
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// Pooled context: acceptance throws in the first lease (tracker left with entries, during-save event buffered),
    /// the lease ends; the next lease of the same instance starts with an empty buffer and writes nothing of the first
    /// lease; the committed row exists exactly once.
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task PooledLease_AcceptanceThrows_NextLeaseStartsClean_CommittedRowOnce(bool async)
    {
        using var host = PoolHost.Create(ContextRegistration.AddDbContextPool);
        var first = host.Lease();
        var thrown = false;
        EventHandler<EntityStateChangedEventArgs> stateChanged = (_, e) =>
        {
            if (!thrown && e.Entry.Entity is User && e.NewState == EntityState.Unchanged)
            {
                thrown = true;
                throw new AcceptanceFailure(nameof(User));
            }
        };
        var calls = 0;
        EventHandler<SavingChangesEventArgs> saving = (_, _) => { if (calls++ == 0) first.Db.Outbox.AddEvent(new PlainEvent { Text = "during save" }); };
        first.Db.ChangeTracker.StateChanged += stateChanged;
        first.Db.SavingChanges += saving;
        var user = new User { Email = "a" };
        first.Db.Users.AddRange(user, new User { Email = "b" });
        first.Db.Outbox.AddEvent(new UserCreated { User = user });

        Assert.IsType<AcceptanceFailure>(await Record.ExceptionAsync(() => Save.Invoke(first.Db, async, true)));
        Assert.Single(first.Db.Outbox.GetEvents());
        first.Db.ChangeTracker.StateChanged -= stateChanged;
        first.Db.SavingChanges -= saving;
        var instance = first.Db;
        await first.End(async);

        var second = host.Lease();
        Assert.Same(instance, second.Db);
        Assert.Empty(second.Db.Outbox.GetEvents());
        Assert.Empty(second.Db.ChangeTracker.Entries());
        second.Db.Users.Add(new User { Email = "c" });
        await Save.Invoke(second.Db, async);
        await second.End(async);

        Assert.Single(host.OutboxRows());
        Assert.DoesNotContain(host.OutboxRows(), r => r.Payload.Contains("during save"));
        Assert.Equal(3, host.Users().Count);
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// Characterization (caller-transaction path, not changed by the fix): acceptance throws in a flag = true save
    /// inside a caller transaction after the User entry was accepted; the caller rolls back. The pending message was
    /// saved with flag = true (accepted), so on release its event leaves the buffer: nothing is written or dispatched
    /// later (no event for the rolled-back user). The user entry stays Unchanged in the tracker (plain EF: its change
    /// is lost, as for any accepted save in a rolled-back transaction).
    /// </summary>
    [Theory]
    [MemberData(nameof(Async))]
    public async Task CallerTransaction_AcceptanceThrows_ThenRollback_EventReleasedAsAccepted_NothingWrittenLater(bool async)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        ThrowOnceWhenAccepted(db, nameof(User));
        var user = new User { Email = "a" };
        db.Users.Add(user);
        db.Outbox.AddEvent(new UserCreated { User = user });

        using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            Assert.IsType<AcceptanceFailure>(await Record.ExceptionAsync(() => Save.Invoke(db, async, true)));
            if (async) await tx.RollbackAsync(); else tx.Rollback();
        }

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Equal(EntityState.Unchanged, db.Entry(user).State);
        Assert.Empty(db.Outbox.GetEvents());
        Assert.Empty(host.Queue.Enqueued);

        await AssertNothingMoreIsWritten(host, db, async);
        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
    }
}
