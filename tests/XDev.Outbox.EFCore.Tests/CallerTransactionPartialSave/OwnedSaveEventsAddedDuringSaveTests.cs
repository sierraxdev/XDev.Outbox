using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.CallerTransactionPartialSave;

/// <summary>
/// fix-owned-save-drops-events-added-during-save: an event added to the buffer while a library-owned save runs
/// (SavingChanges/SavedChanges handler, ISaveChangesInterceptor collecting domain events) is not part of that save:
/// the events to save are collected before EF raises any save event. The owned save removes only the events it wrote
/// (by reference and multiplicity); every other event stays buffered and is written once by the next save, as on the
/// caller-transaction and no-events paths. Before the fix the owned save cleared the whole buffer and such an event
/// was dropped without a row.
/// </summary>
public class OwnedSaveEventsAddedDuringSaveTests
{
    public static TheoryData<bool, bool> AsyncFlag()
    {
        var data = new TheoryData<bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
            data.Add(async, flag);
        return data;
    }

    public static TheoryData<bool> Async() => new() { false, true };

    private static User AddUserWithEvent(AdvDbContext db, string email, out UserCreated evt)
    {
        var user = new User { Email = email };
        db.Users.Add(user);
        evt = new UserCreated { User = user };
        db.Outbox.AddEvent(evt);
        return user;
    }

    /// <summary>Owned save with the given flag; a flag = false save is followed by AcceptAllChanges (as plain EF guidance).</summary>
    private static async Task SaveAndAccept(AdvDbContext db, bool async, bool flag)
    {
        await Save.Invoke(db, async, flag);
        if (!flag) db.ChangeTracker.AcceptAllChanges();
    }

    /// <summary>Every user row carries the key of its committed user; every outbox row was dispatched exactly once.</summary>
    private static void AssertUserRowsMatchAndEachRowDispatchedOnce(CommitTesterHost host)
    {
        var users = host.Users();
        foreach (var row in host.UserRows())
        {
            var payload = Payload.User(row.Payload);
            var email = payload.GetProperty("email").GetString();
            var user = Assert.Single(users, u => u.Email == email);
            Assert.Equal(user.Id, payload.GetProperty("id").GetInt32());
        }
        Assert.Equal(host.OutboxRows().Select(r => r.Id), host.Queue.Enqueued.Select(m => m.Id).OrderBy(x => x));
    }

    private static int RowsWithText(CommitTesterHost host, string text) => host.OutboxRows().Count(r => r.Payload.Contains(text));

    /// <summary>A later unrelated save writes and dispatches nothing more and leaves the buffer empty.</summary>
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

    // ---------------- Events added during an owned save are kept ----------------

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task SavingChangesHandler_EventAddedDuringOwnedSave_KeptAndWrittenOnceByNextSave(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        PlainEvent? during = null;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(during = new PlainEvent { Text = "during save" });
        };

        AddUserWithEvent(db, "a", out _);
        await SaveAndAccept(db, async, flag);

        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Equal(0, RowsWithText(host, "during save"));
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);

        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task SavedChangesHandler_EventAddedDuringOwnedSave_KeptAndWrittenOnceByNextSave(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        PlainEvent? during = null;
        db.SavedChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(during = new PlainEvent { Text = "after save" });
        };

        AddUserWithEvent(db, "a", out _);
        await SaveAndAccept(db, async, flag);

        Assert.Equal(1, added);
        Assert.Single(host.OutboxRows());
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);

        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(1, RowsWithText(host, "after save"));
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// A SaveChangesInterceptor that raises a domain event for every new user (the common "collect domain events in
    /// SavingChanges" pattern), during an owned save that writes another event. The domain event is not part of the save
    /// that raised it; it stays buffered and the next save writes it once, carrying the user's generated key.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task SaveChangesInterceptorCollectingDomainEvents_EventKept_NextSaveWritesItOnceWithGeneratedKey(bool async, bool flag)
    {
        var collector = new UserCreatedCollector();
        using var host = CommitTesterHost.Create(extraInterceptors: new IInterceptor[] { collector });
        var (scope, db) = host.CreateScope();
        using var _s = scope;

        var user = new User { Email = "domain" };
        db.Users.Add(user);
        db.Outbox.AddEvent(new PlainEvent { Text = "explicit" });
        await SaveAndAccept(db, async, flag);

        Assert.Equal(1, collector.Raised);
        var dbUser = Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Equal(1, RowsWithText(host, "explicit"));
        var kept = Assert.IsType<UserCreated>(Assert.Single(db.Outbox.GetEvents()));
        Assert.Same(user, kept.User);
        Assert.Equal(dbUser.Id, user.Id);

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);

        Assert.Equal(1, collector.Raised);
        Assert.Equal(2, host.OutboxRows().Count);
        var row = Assert.Single(host.UserRows());
        var payload = Payload.User(row.Payload);
        Assert.Equal(dbUser.Id, payload.GetProperty("id").GetInt32());
        Assert.Equal("domain", payload.GetProperty("email").GetString());
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Multiplicity ----------------

    /// <summary>Characterization (same before and after the fix): the same instance added twice is written twice and both copies leave the buffer.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task SameInstanceAddedTwiceBeforeOwnedSave_BothWrittenAndBothRemoved(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        AddUserWithEvent(db, "a", out var evt);
        db.Outbox.AddEvent(evt);

        await SaveAndAccept(db, async, flag);

        Assert.Equal(2, host.UserRows().Count);
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// A handler re-adds the instance the save is writing: the save wrote one copy and removes one copy; the copy added
    /// during the save stays buffered and the next save writes it (two rows in total, as added twice).
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task CapturedInstanceReAddedDuringOwnedSave_OneCopyKept_NextSaveWritesSecondRow(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        AddUserWithEvent(db, "a", out var evt);
        var added = 0;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(evt);
        };

        await SaveAndAccept(db, async, flag);

        Assert.Single(host.UserRows());
        Assert.Same(evt, Assert.Single(db.Outbox.GetEvents()));

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);

        Assert.Equal(2, host.UserRows().Count);
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    // ---------------- Retrying execution strategy ----------------

    /// <summary>
    /// Retrying strategy, first commit fails: the events to save are collected once, outside the strategy, so the
    /// event a guarded handler added on the first attempt is not part of the retried save either; it stays buffered
    /// once and the next save writes it once. The user and its event are committed and dispatched once.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task RetryingStrategyCommitFailsOnce_GuardedHandlerEvent_KeptOnce_NextSaveWritesItOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        PlainEvent? during = null;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(during = new PlainEvent { Text = "during save" });
        };
        AddUserWithEvent(db, "a", out _);

        host.Faults.FailCommits();
        await SaveAndAccept(db, async, flag);

        Assert.Equal(1, host.Faults.CommitFailuresFired);
        Assert.Equal(2, added); // the handler ran on both attempts
        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Single(host.OutboxRows());
        Assert.Same(during, Assert.Single(db.Outbox.GetEvents()));

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);

        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// CONSUMER HAZARD (characterization of the approved fix): an unguarded handler adds an event on every attempt of a
    /// retried owned save, so one copy per attempt stays buffered and the next save writes them all.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task RetryingStrategyCommitFailsOnce_UnguardedHandler_OneCopyPerAttemptKept_NextSaveWritesEach(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create(retrying: true);
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        EventHandler<SavingChangesEventArgs> handler = (_, _) => db.Outbox.AddEvent(new PlainEvent { Text = "per attempt" });
        db.SavingChanges += handler;
        AddUserWithEvent(db, "a", out _);

        host.Faults.FailCommits();
        await SaveAndAccept(db, async, flag);
        db.SavingChanges -= handler;

        Assert.Equal(1, host.Faults.CommitFailuresFired);
        Assert.Single(host.Users());
        Assert.Single(host.OutboxRows());
        Assert.Equal(2, db.Outbox.GetEvents().Count);

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "later" });
        await Save.Invoke(db, async);

        Assert.Equal(3, host.OutboxRows().Count);
        Assert.Equal(2, RowsWithText(host, "per attempt"));
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
    }

    // ---------------- Characterization: same before and after the fix ----------------

    /// <summary>A failed owned save (no retry) keeps both the event it tried to save and the one added during it (D2); the next save writes each once.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task FailedOwnedSave_HandlerEvent_BothKept_NextSaveWritesEachOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" });
        };
        AddUserWithEvent(db, "a", out var evt);

        host.Faults.FailCommits();
        var error = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));

        Assert.True(ExceptionChain.Contains<InjectedFailureException>(error));
        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        var buffered = db.Outbox.GetEvents();
        Assert.Equal(2, buffered.Count);
        Assert.Same(evt, buffered.First());

        await SaveAndAccept(db, async, flag);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>A save without events passes through to EF and never touches the buffer: the event a handler adds is kept and written once by the next save.</summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task NoEventsSave_HandlerAddsEvent_KeptAndWrittenOnceByNextSave(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" });
        };

        db.Docs.Add(new Doc { Id = Guid.NewGuid(), Title = "first" });
        await SaveAndAccept(db, async, flag);

        Assert.Empty(host.OutboxRows());
        Assert.Single(db.Outbox.GetEvents());

        await Save.Invoke(db, async);

        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Single(host.OutboxRows());
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }

    /// <summary>
    /// A flag = false save in a caller transaction (with a handler adding an event during it), then the transaction is
    /// disposed without commit or rollback (no EF event). The next owned save releases the stale pending message and
    /// writes the released event and the handler's event once each, with the user's generated key; nothing is kept.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsyncFlag))]
    public async Task StalePendingMessage_ThenOwnedSave_WritesReleasedAndHandlerEventsOnce(bool async, bool flag)
    {
        using var host = CommitTesterHost.Create();
        var (scope, db) = host.CreateScope();
        using var _s = scope;
        var added = 0;
        db.SavingChanges += (_, _) =>
        {
            if (added++ == 0) db.Outbox.AddEvent(new PlainEvent { Text = "during save" });
        };
        AddUserWithEvent(db, "a", out _);

        IDbContextTransaction tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();
        await Save.Invoke(db, async, false);
        if (async) await tx.DisposeAsync(); else tx.Dispose();

        Assert.Empty(host.Users());
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(2, db.Outbox.GetEvents().Count);

        await SaveAndAccept(db, async, flag);

        Assert.Single(host.Users());
        Assert.Single(host.UserRows());
        Assert.Equal(1, RowsWithText(host, "during save"));
        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Empty(db.Outbox.GetEvents());
        AssertUserRowsMatchAndEachRowDispatchedOnce(host);
        await AssertNothingMoreIsWritten(host, db, async);
    }
}

/// <summary>Raises a <see cref="UserCreated"/> domain event, once per user instance, for every new user in SavingChanges.</summary>
public sealed class UserCreatedCollector : SaveChangesInterceptor
{
    private readonly HashSet<User> _raised = new(ReferenceEqualityComparer.Instance);

    public int Raised => _raised.Count;

    private void Collect(DbContext? context)
    {
        if (context is not AdvDbContext db) return;
        foreach (var entry in db.ChangeTracker.Entries<User>().Where(e => e.State == EntityState.Added).ToList())
        {
            if (_raised.Add(entry.Entity)) db.Outbox.AddEvent(new UserCreated { User = entry.Entity });
        }
    }

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        Collect(eventData.Context);
        return result;
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        Collect(eventData.Context);
        return ValueTask.FromResult(result);
    }
}
