using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Adversarial;

using Host = AdvHost<AdvDbContext, int>;

/// <summary>Adversarial verification of the save-overload / deferred-acceptance fix.</summary>
public class AdversarialSaveTests
{
    public static TheoryData<bool, bool, bool> AsyncFlagAmbient()
    {
        var data = new TheoryData<bool, bool, bool>();
        foreach (var async in new[] { false, true })
        foreach (var flag in new[] { false, true })
        foreach (var ambient in new[] { false, true })
            data.Add(async, flag, ambient);
        return data;
    }

    private static async Task<int> SaveIn(DbContext db, bool async, bool flag, bool ambient)
    {
        if (!ambient) return await Save.Invoke(db, async, flag);
        using var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction();
        var result = await Save.Invoke(db, async, flag);
        if (async) await tx.CommitAsync(); else tx.Commit();
        return result;
    }

    private static JsonElement Root(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    // ---------------- Rule A projection ----------------

    [Theory]
    [MemberData(nameof(AsyncFlagAmbient))]
    public async Task Save_EventReferencingModifiedEntityWithComputedColumn_PayloadCarriesNewComputedValueAndClrIsRestoredWhenNotAccepted(bool async, bool flag, bool ambient)
    {
        using var host = Host.Create();
        host.Exec("INSERT INTO \"Products\" (\"Name\", \"Price\") VALUES ('p', 1)");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var product = await db.Products.SingleAsync();
        Assert.Equal(2, product.Doubled);
        product.Price = 5;
        outbox.AddEvent(new ProductChanged { Product = product });

        await SaveIn(db, async, flag, ambient);

        Assert.Equal(10, host.Scalar("SELECT \"Doubled\" FROM \"Products\""));
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(10, Root(row.Payload).GetProperty("product").GetProperty("doubled").GetInt32());

        if (flag)
        {
            Assert.Equal(EntityState.Unchanged, db.Entry(product).State);
            Assert.Equal(10, product.Doubled);
        }
        else
        {
            Assert.Equal(EntityState.Modified, db.Entry(product).State);
            Assert.Equal(2, product.Doubled); // CLR value restored after projection
            db.ChangeTracker.AcceptAllChanges();
            Assert.Equal(10, product.Doubled);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_EventNotReferencingAnyEntity_RunsNoPayloadUpdate(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        db.Users.Add(new User { Email = "a@test" });
        outbox.AddEvent(new PlainEvent { Text = "hello" });

        var result = await Save.Invoke(db, async, acceptAllChangesOnSuccess: false);

        Assert.Equal(1, result);
        Assert.Equal(0, host.OutboxUpdateCommands());
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal("hello", Root(row.Payload).GetProperty("text").GetString());
        Assert.Single(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_EventReferencingEntitiesWithClientOrCompositeKeys_RunsNoPayloadUpdateAndPersistsKeys(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var doc = new Doc { Title = "d" };
        var line = new Line { OrderNo = 7, LineNo = 3, Sku = "s" };
        db.Docs.Add(doc);
        db.Lines.Add(line);
        Assert.NotEqual(Guid.Empty, doc.Id); // client generated on Add
        outbox.AddEvent(new DocCreated { Doc = doc }, new LineAdded { Line = line });

        await Save.Invoke(db, async, acceptAllChangesOnSuccess: true);

        Assert.Equal(0, host.OutboxUpdateCommands());
        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        Assert.Equal(doc.Id, Root(rows[0].Payload).GetProperty("doc").GetProperty("id").GetGuid());
        Assert.Equal(3, Root(rows[1].Payload).GetProperty("line").GetProperty("lineNo").GetInt32());
        Assert.Equal(2, host.Queue.Enqueued.Count);
    }

    [Theory]
    [MemberData(nameof(AsyncFlagAmbient))]
    public async Task Save_EventReferencingEntityWithOwnedType_PayloadHasGeneratedKeyAndOwnedValues(bool async, bool flag, bool ambient)
    {
        using var host = Host.Create();
        host.Exec("INSERT INTO \"Customers\" (\"Name\", \"Address_City\") VALUES ('seed', 'x')");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var customer = new Customer { Name = "c", Address = new Address { City = "Rome" } };
        db.Customers.Add(customer);
        outbox.AddEvent(new CustomerCreated { Customer = customer });

        await SaveIn(db, async, flag, ambient);

        var id = host.Scalar("SELECT MAX(\"Id\") FROM \"Customers\"");
        Assert.Equal(2, id);
        var payload = Root(Assert.Single(host.OutboxRows()).Payload).GetProperty("customer");
        Assert.Equal(id, payload.GetProperty("id").GetInt32());
        Assert.Equal("Rome", payload.GetProperty("address").GetProperty("city").GetString());
        Assert.Equal(flag ? 2 : 0, customer.Id);
    }

    [Theory]
    [MemberData(nameof(AsyncFlagAmbient))]
    public async Task Save_EventReferencingUsersViaCollectionAndNestedGraph_AllPayloadsCarryRealKeys(bool async, bool flag, bool ambient)
    {
        using var host = Host.Create();
        host.Exec("INSERT INTO \"Users\" (\"Email\") VALUES ('seed')");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var a = new User { Email = "a" };
        var b = new User { Email = "b" };
        db.Users.AddRange(a, b);
        outbox.AddEvent(new UsersImported { Users = { a, b } }, new Envelope { Inner = { User = b } });

        var result = await SaveIn(db, async, flag, ambient);

        Assert.Equal(2, result);
        var ids = new[] { 2, 3 };
        Assert.Equal(3, host.Scalar("SELECT MAX(\"Id\") FROM \"Users\""));
        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        var listed = Root(rows[0].Payload).GetProperty("users").EnumerateArray().Select(x => x.GetProperty("id").GetInt32()).ToArray();
        Assert.Equal(ids, listed);
        Assert.Equal(3, Root(rows[1].Payload).GetProperty("inner").GetProperty("user").GetProperty("id").GetInt32());
        Assert.Equal(2, host.Queue.Enqueued.Count);
        Assert.Equal(rows.Select(r => int.Parse(r.Id)), host.Queue.Enqueued.Select(m => m.Id));
        Assert.Equal(flag ? 2 : 0, a.Id);
        Assert.Equal(flag ? 3 : 0, b.Id);
    }

    [Theory]
    [MemberData(nameof(AsyncFlagAmbient))]
    public async Task Save_EventReferencingNotificationTrackedEntity_PayloadHasKeyAndTrackerStaysConsistent(bool async, bool flag, bool ambient)
    {
        using var host = Host.Create();
        host.Exec("INSERT INTO \"NotifyItems\" (\"Label\") VALUES ('seed')");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var item = new NotifyItem { Label = "n" };
        db.NotifyItems.Add(item);
        outbox.AddEvent(new NotifyItemCreated { Item = item });

        await SaveIn(db, async, flag, ambient);

        Assert.Equal(2, host.Scalar("SELECT COUNT(*) FROM \"NotifyItems\""));
        Assert.Equal(2, Root(Assert.Single(host.OutboxRows()).Payload).GetProperty("item").GetProperty("id").GetInt32());

        if (flag)
        {
            Assert.Equal(EntityState.Unchanged, db.Entry(item).State);
            Assert.Equal(2, item.Id);
        }
        else
        {
            Assert.Equal(EntityState.Added, db.Entry(item).State);
            Assert.Equal(0, item.Id);
            db.ChangeTracker.AcceptAllChanges();
            Assert.Equal(EntityState.Unchanged, db.Entry(item).State);
            Assert.Equal(2, item.Id);
        }

        // A later save writes nothing more for the item.
        await Save.Invoke(db, async);
        Assert.Equal(2, host.Scalar("SELECT COUNT(*) FROM \"NotifyItems\""));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_EventReferencingEntityWithSideEffectingKeySetter_PayloadHasGeneratedKey(bool async)
    {
        using var host = Host.Create();

        {
            var (scope, db, _) = host.CreateScope();
            using var s = scope;
            var plain = new SideEffectItem { Name = "baseline" };
            db.SideEffectItems.Add(plain);
            await Save.Invoke(db, async);
        }

        var (scope2, db2, outbox2) = host.CreateScope();
        using var s2 = scope2;
        var item = new SideEffectItem { Name = "evented" };
        db2.SideEffectItems.Add(item);
        outbox2.AddEvent(new SideEffectItemCreated { Item = item });
        await Save.Invoke(db2, async);

        Assert.Equal(2, Root(Assert.Single(host.OutboxRows()).Payload).GetProperty("item").GetProperty("id").GetInt32());
    }

    // ---------------- Failure restoration ----------------

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task OwnedSave_WhenPayloadReserializationThrows_RollsBackRestoresClrAndTrackerAndKeepsBuffer(bool async, bool flag)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        user.Orders.Add(new Order { Description = "o" });
        db.Users.Add(user);
        var evt = new ExplodingAfterKeyEvent { User = user };
        outbox.AddEvent(evt);

        var ex = await Record.ExceptionAsync(() => Save.Invoke(db, async, flag));

        Assert.NotNull(ex);
        Assert.True(ExceptionChain.Contains<InvalidOperationException>(ex), ex.ToString());
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Orders\""));
        Assert.Empty(host.OutboxRows());
        Assert.Empty(host.Queue.Enqueued);
        Assert.Equal(0, user.Id);
        Assert.Equal(0, user.Orders[0].UserId);
        Assert.Equal(EntityState.Added, db.Entry(user).State);
        Assert.Equal(EntityState.Added, db.Entry(user.Orders[0]).State);
        Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
        Assert.Same(evt, Assert.Single(outbox.GetEvents()));

        // Application drops the bad event and retries: exactly one user/order, real keys, no outbox row.
        outbox.ClearEvents();
        outbox.AddEvent(new UserCreated { User = user });
        await Save.Invoke(db, async);
        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Orders\""));
        Assert.Equal(1, user.Id);
        Assert.Equal(1, Payload.User(Assert.Single(host.OutboxRows()).Payload).GetProperty("id").GetInt32());
        Assert.Single(host.Queue.Enqueued);
    }

    // ---------------- Return values / multiplicity ----------------

    [Theory]
    [MemberData(nameof(AsyncFlagAmbient))]
    public async Task Save_WithTwoBusinessRowsAndTwoEvents_ReturnsBusinessRowCountOnly(bool async, bool flag, bool ambient)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        user.Orders.Add(new Order { Description = "o" });
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user }, new PlainEvent { Text = "x" });

        var result = await SaveIn(db, async, flag, ambient);

        Assert.Equal(2, result);
        Assert.Equal(2, host.OutboxRows().Count);
        Assert.Equal(2, host.Queue.Enqueued.Count);
        Assert.Empty(outbox.GetEvents());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Save_SameEventInstanceAddedTwice_WritesTwoRowsAndEnqueuesTwoDistinctIds(bool async, bool ambient)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        var evt = new UserCreated { User = user };
        outbox.AddEvent(evt);
        outbox.AddEvent(evt);

        await SaveIn(db, async, flag: true, ambient);

        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(1, Payload.User(r.Payload).GetProperty("id").GetInt32()));
        Assert.Equal(rows.Select(r => int.Parse(r.Id)), host.Queue.Enqueued.Select(m => m.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_ContextReusedForManySaves_EachSaveWritesAndDispatchesOnlyItsOwnEvent(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        for (var i = 1; i <= 5; i++)
        {
            var user = new User { Email = "u" + i };
            db.Users.Add(user);
            outbox.AddEvent(new UserCreated { User = user });
            var result = await Save.Invoke(db, async, acceptAllChangesOnSuccess: i % 2 == 0);
            if (i % 2 != 0) db.ChangeTracker.AcceptAllChanges();

            Assert.Equal(1, result);
            Assert.Equal(i, user.Id);
            Assert.Empty(outbox.GetEvents());
            Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
        }

        var rows = host.OutboxRows();
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, rows.Select(r => Payload.User(r.Payload).GetProperty("id").GetInt32()));
        Assert.Equal(rows.Select(r => int.Parse(r.Id)), host.Queue.Enqueued.Select(m => m.Id));
        Assert.Equal(5, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
    }

    // ---------------- Ambient transactions ----------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_TwoSequentialTransactionsOnSameContext_EachCommitDispatchesOnlyItsOwnMessage(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        for (var i = 1; i <= 2; i++)
        {
            var user = new User { Email = "u" + i };
            db.Users.Add(user);
            outbox.AddEvent(new UserCreated { User = user });
            await SaveIn(db, async, flag: true, ambient: true);
            Assert.Equal(i, host.Queue.Enqueued.Count);
        }

        var rows = host.OutboxRows();
        Assert.Equal(rows.Select(r => int.Parse(r.Id)), host.Queue.Enqueued.Select(m => m.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_InExternalDbTransactionViaUseTransaction_CommitThroughEfDispatchesOnce(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        await db.Database.OpenConnectionAsync();
        using var external = host.Connection.BeginTransaction();
        var efTx = async ? await db.Database.UseTransactionAsync(external) : db.Database.UseTransaction(external);
        Assert.NotNull(efTx);

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        await Save.Invoke(db, async);
        Assert.Empty(host.Queue.Enqueued);

        if (async) await efTx!.CommitAsync(); else efTx!.Commit();

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(int.Parse(row.Id), Assert.Single(host.Queue.Enqueued).Id);
        Assert.Equal(1, Payload.User(row.Payload).GetProperty("id").GetInt32());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_ThenSaveAgainWithoutEventsInSameTransaction_CommitDispatchesOnce(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            var user = new User { Email = "a" };
            db.Users.Add(user);
            outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(db, async);
            user.Email = "changed";
            await Save.Invoke(db, async);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
        Assert.Empty(outbox.GetEvents());
    }

    /// <summary>
    /// A caller rolls back to a savepoint taken before the save: the outbox row no longer exists, so its
    /// message must not be dispatched when the outer transaction commits.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_RolledBackToCallerSavepoint_CommitDoesNotDispatchMessageWhoseRowWasRolledBack(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            if (async) await tx.CreateSavepointAsync("before"); else tx.CreateSavepoint("before");

            var user = new User { Email = "a" };
            db.Users.Add(user);
            outbox.AddEvent(new UserCreated { User = user });
            await Save.Invoke(db, async);

            if (async) await tx.RollbackToSavepointAsync("before"); else tx.RollbackToSavepoint("before");
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        Assert.Empty(host.OutboxRows());
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Empty(host.Queue.Enqueued);
    }

    /// <summary>
    /// D5 (deferred): inside a caller transaction the library's payload UPDATE runs after EF's own savepoint is
    /// released, so a failure there leaves business and outbox rows in the caller's transaction while the
    /// tracker still says Added. Plain EF semantics would make retrying the save safe.
    /// </summary>
    [Theory(Skip = "D5 deferred (no ambient savepoint). Confirmed failing on net8.0-net11.0: 2 Users rows after the caller's retry and commit.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientSave_WhenPayloadUpdateFailsAndCallerRetriesThenCommits_NoDuplicateRows(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });
        host.Faults.FailCommands(SqlMatch.IsOutboxUpdate);

        using (var tx = async ? await db.Database.BeginTransactionAsync() : db.Database.BeginTransaction())
        {
            var ex = await Record.ExceptionAsync(() => Save.Invoke(db, async));
            Assert.NotNull(ex);
            Assert.Equal(1, host.Faults.CommandFailuresFired);
            await Save.Invoke(db, async);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }

        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    // ---------------- Cancellation ----------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SaveAsync_WithCancelledToken_ThrowsWritesNothingKeepsBufferAndLaterSaveSucceeds(bool ambient)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        IDbContextTransaction? tx = ambient ? await db.Database.BeginTransactionAsync() : null;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.SaveChangesAsync(cts.Token));

        Assert.Equal(EntityState.Added, db.Entry(user).State);
        Assert.Equal(0, user.Id);
        Assert.Single(outbox.GetEvents());
        Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
        Assert.Empty(host.Queue.Enqueued);

        await db.SaveChangesAsync();
        if (tx != null) { await tx.CommitAsync(); tx.Dispose(); }

        Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Users\""));
        Assert.Equal(1, Payload.User(Assert.Single(host.OutboxRows()).Payload).GetProperty("id").GetInt32());
        Assert.Single(host.Queue.Enqueued);
    }

    // ---------------- Retry with several events ----------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSave_PayloadUpdateFailsForSecondMessage_AppRetryWritesEachPayloadWithItsOwnKeys(bool async)
    {
        using var host = Host.Create();
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var a = new User { Email = "a" };
        var b = new User { Email = "b" };
        db.Users.AddRange(a, b);
        outbox.AddEvent(new UserCreated { User = a }, new UserCreated { User = b });

        var updates = 0;
        host.Faults.FailCommands(sql => SqlMatch.IsOutboxUpdate(sql) && ++updates == 2);

        await Assert.ThrowsAnyAsync<Exception>(() => Save.Invoke(db, async));
        Assert.Empty(host.OutboxRows());
        Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"Users\""));

        await Save.Invoke(db, async);

        var rows = host.OutboxRows();
        Assert.Equal(2, rows.Count);
        Assert.Equal(a.Id, Payload.User(rows[0].Payload).GetProperty("id").GetInt32());
        Assert.Equal(b.Id, Payload.User(rows[1].Payload).GetProperty("id").GetInt32());
        Assert.NotEqual(a.Id, b.Id);
        Assert.Equal(2, host.Queue.Enqueued.Count);
    }

    // ---------------- Outbox table / key configuration ----------------

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Save_WithCustomOutboxTableName_PayloadUpdateTargetsConfiguredTable(bool async, bool flag)
    {
        using var host = AdvHost<CustomTableDbContext, int>.Create(CustomTableDbContext.Table, o => o.TableName = CustomTableDbContext.Table);
        host.Exec("INSERT INTO \"Users\" (\"Email\") VALUES ('seed')");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });

        await Save.Invoke(db, async, flag);

        Assert.Equal(1, host.OutboxUpdateCommands());
        Assert.Equal(2, Payload.User(Assert.Single(host.OutboxRows()).Payload).GetProperty("id").GetInt32());
        Assert.Single(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Save_WithOutboxTableNameContainingBraces_SucceedsAndPersistsRealKey(bool async)
    {
        using var host = AdvHost<BraceTableDbContext, int>.Create(BraceTableDbContext.Table, o => o.TableName = BraceTableDbContext.Table);
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });

        await Save.Invoke(db, async);

        Assert.Equal(1, Payload.User(Assert.Single(host.OutboxRows()).Payload).GetProperty("id").GetInt32());
        Assert.Single(host.Queue.Enqueued);
    }

    [Theory]
    [MemberData(nameof(AsyncFlagAmbient))]
    public async Task Save_WithGuidOutboxKey_PersistsPayloadWithGeneratedKeyAndEnqueuesRowId(bool async, bool flag, bool ambient)
    {
        using var host = AdvHost<GuidOutboxDbContext, Guid>.Create();
        host.Exec("INSERT INTO \"Users\" (\"Email\") VALUES ('seed')");
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var user = new User { Email = "a" };
        db.Users.Add(user);
        outbox.AddEvent(new UserCreated { User = user });

        await SaveIn(db, async, flag, ambient);

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(2, Payload.User(row.Payload).GetProperty("id").GetInt32());
        var message = Assert.Single(host.Queue.Enqueued);
        Assert.NotEqual(Guid.Empty, message.Id);
        Assert.Equal(Guid.Parse(row.Id), message.Id);
    }
}
