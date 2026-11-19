using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Adversarial;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.Round2;

public enum GraphScenario
{
    /// <summary>Company -> 2 Depts -> 3 Employees (one manages another), Company -> Guid-keyed Folder -> 2 identity FileItems.</summary>
    DeepGraphWithSelfReferenceAndGuidKeys,
    /// <summary>New Post with two new Tags and one existing Tag (property-bag join entities).</summary>
    ManyToMany,
    /// <summary>Owner with an owned collection whose items have their own identity key.</summary>
    OwnedCollectionWithGeneratedKey,
    /// <summary>New Dept (with new Employee) added under an existing, Unchanged Company.</summary>
    AddedDependentOfUnchangedPrincipal,
    /// <summary>Existing (Modified) Employee moved to a newly Added Dept: its FK points to a temporary key.</summary>
    ModifiedDependentReparentedToAddedPrincipal,
    /// <summary>Two full-notification entities whose keys have no discoverable backing field (setter fallback).</summary>
    NotificationEntitiesWithoutBackingField,
    /// <summary>Two snapshot-tracked entities whose keys have no discoverable backing field (setter fallback).</summary>
    SideEffectEntitiesWithoutBackingField,
}

/// <summary>
/// Correction round, pass "generated-value snapshot/restore": a failure after base.SaveChanges(false) succeeded
/// (payload UPDATE or commit) must leave the tracker so that the next attempt (strategy retry or application retry,
/// one or two failures first) inserts the whole graph again with fresh store-generated keys, consistent FKs,
/// no explicit identity values, and a payload carrying the real keys.
/// </summary>
public class GeneratedValueRestoreTests
{
    public static TheoryData<GraphScenario, FailurePoint, bool, bool, bool> Cases()
    {
        var data = new TheoryData<GraphScenario, FailurePoint, bool, bool, bool>();
        foreach (var scenario in Enum.GetValues<GraphScenario>())
        foreach (var failure in new[] { FailurePoint.PayloadUpdate, FailurePoint.Commit })
        foreach (var strategy in new[] { false, true })
        foreach (var flag in new[] { false, true })
        foreach (var async in new[] { false, true })
            data.Add(scenario, failure, strategy, flag, async);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task OwnedSave_TwoConsecutiveFailuresAfterInsert_ThenSuccess_GraphPersistedOnceWithConsistentKeys(
        GraphScenario scenario, FailurePoint failure, bool strategy, bool flag, bool async)
    {
        using var host = R2Host.Create(retryingStrategy: strategy);
        var seed = Seed(host, scenario);
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var verify = Arrange(db, outbox, scenario, seed);

        if (failure == FailurePoint.PayloadUpdate) host.Faults.FailCommands(SqlMatch.IsOutboxUpdate, times: 2);
        else host.Faults.FailCommits(times: 2);

        if (strategy)
        {
            await R2Save.Invoke(db, async, flag);
        }
        else
        {
            for (var i = 0; i < 2; i++)
            {
                var ex = await Record.ExceptionAsync(() => R2Save.Invoke(db, async, flag));
                Assert.NotNull(ex);
                Assert.True(ExceptionChain.Contains<InjectedFailureException>(ex), ex.ToString());
                Assert.IsNotType<AggregateException>(ex);
                Assert.Equal(0, host.Scalar("SELECT COUNT(*) FROM \"OutboxMessages\""));
                Assert.Empty(host.Queue.Enqueued);
                Assert.Single(outbox.GetEvents());
            }
            await R2Save.Invoke(db, async, flag);
        }

        Assert.Equal(2, failure == FailurePoint.PayloadUpdate ? host.Faults.CommandFailuresFired : host.Faults.CommitFailuresFired);
        if (!flag) db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(0, host.ForeignKeyViolations());
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
        Assert.Empty(outbox.GetEvents());
        Assert.All(db.ChangeTracker.Entries(), e => Assert.Equal(EntityState.Unchanged, e.State));

        verify(host, R2Host.Root(row.Payload));
    }

    public static TheoryData<GraphScenario, bool, bool> AmbientCases()
    {
        var data = new TheoryData<GraphScenario, bool, bool>();
        foreach (var scenario in Enum.GetValues<GraphScenario>())
        foreach (var flag in new[] { false, true })
        foreach (var async in new[] { false, true })
            data.Add(scenario, flag, async);
        return data;
    }

    /// <summary>
    /// Caller-owned transaction: the outbox INSERT fails, so EF rolls back to its internal savepoint (business rows
    /// undone). The caller retries the save in the same transaction and commits: the graph is inserted once with fresh
    /// keys and consistent FKs.
    /// </summary>
    [Theory]
    [MemberData(nameof(AmbientCases))]
    public async Task AmbientSave_OutboxInsertFailsTwice_CallerRetriesInSameTransaction_GraphPersistedOnce(GraphScenario scenario, bool flag, bool async)
    {
        using var host = R2Host.Create();
        var seed = Seed(host, scenario);
        var (scope, db, outbox) = host.CreateScope();
        using var _ = scope;

        var verify = Arrange(db, outbox, scenario, seed);
        host.Faults.FailCommands(SqlMatch.IsOutboxInsert, times: 2);

        using (var tx = await R2Save.Begin(db, async))
        {
            for (var i = 0; i < 2; i++)
            {
                var ex = await Record.ExceptionAsync(() => R2Save.Invoke(db, async, flag));
                Assert.True(ExceptionChain.Contains<InjectedFailureException>(ex), ex?.ToString());
                Assert.Empty(db.ChangeTracker.Entries<OutboxMessage<int>>());
            }
            await R2Save.Invoke(db, async, flag);
            await R2Save.Commit(tx, async);
        }
        if (!flag) db.ChangeTracker.AcceptAllChanges();

        Assert.Equal(2, host.Faults.CommandFailuresFired);
        Assert.Equal(0, host.ForeignKeyViolations());
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(row.Id, Assert.Single(host.Queue.Enqueued).Id);
        Assert.Empty(outbox.GetEvents());
        Assert.All(db.ChangeTracker.Entries(), e => Assert.Equal(EntityState.Unchanged, e.State));
        verify(host, R2Host.Root(row.Payload));
    }

    private sealed record SeedIds(int CompanyId, int DeptId, int EmployeeId, int TagId);

    private static SeedIds Seed(R2Host host, GraphScenario scenario)
    {
        switch (scenario)
        {
            case GraphScenario.ManyToMany:
                host.Exec("INSERT INTO \"Tags\" (\"Name\") VALUES ('existing')");
                return new SeedIds(0, 0, 0, (int)host.Scalar("SELECT MAX(\"Id\") FROM \"Tags\""));
            case GraphScenario.AddedDependentOfUnchangedPrincipal:
            case GraphScenario.ModifiedDependentReparentedToAddedPrincipal:
                // Pad ids so that store-generated values differ between tables.
                host.Exec("INSERT INTO \"Companies\" (\"Name\") VALUES ('pad'), ('pad'), ('pad'), ('seed')");
                var companyId = (int)host.Scalar("SELECT MAX(\"Id\") FROM \"Companies\"");
                host.Exec($"INSERT INTO \"Depts\" (\"CompanyId\", \"Name\") VALUES ({companyId}, 'old')");
                var deptId = (int)host.Scalar("SELECT MAX(\"Id\") FROM \"Depts\"");
                host.Exec($"INSERT INTO \"Employees\" (\"DeptId\", \"Name\") VALUES ({deptId}, 'mover')");
                return new SeedIds(companyId, deptId, (int)host.Scalar("SELECT MAX(\"Id\") FROM \"Employees\""), 0);
            default:
                return new SeedIds(0, 0, 0, 0);
        }
    }

    private static Action<R2Host, System.Text.Json.JsonElement> Arrange(R2DbContext db, IOutbox outbox, GraphScenario scenario, SeedIds seed)
    {
        switch (scenario)
        {
            case GraphScenario.DeepGraphWithSelfReferenceAndGuidKeys:
            {
                var manager = new Employee { Name = "m" };
                var report = new Employee { Name = "r", Manager = manager };
                var other = new Employee { Name = "o" };
                var d1 = new Dept { Name = "d1", Employees = { manager, report } };
                var d2 = new Dept { Name = "d2", Employees = { other } };
                var folder = new Folder { Name = "f", Files = { new FileItem { Name = "a" }, new FileItem { Name = "b" } } };
                var company = new Company { Name = "c", Depts = { d1, d2 }, Folders = { folder } };
                db.Companies.Add(company);
                outbox.AddEvent(new CompanyCreated { Company = company });

                return (host, payload) =>
                {
                    var companyId = Assert.Single(host.Ints("SELECT \"Id\" FROM \"Companies\""));
                    Assert.Equal(companyId, company.Id);
                    Assert.Equal(new[] { companyId, companyId }, host.Ints("SELECT \"CompanyId\" FROM \"Depts\" ORDER BY \"Id\""));
                    Assert.Equal(3, host.Scalar("SELECT COUNT(*) FROM \"Employees\""));
                    Assert.Equal(d1.Id, host.Scalar($"SELECT \"DeptId\" FROM \"Employees\" WHERE \"Id\" = {manager.Id}"));
                    Assert.Equal(d1.Id, host.Scalar($"SELECT \"DeptId\" FROM \"Employees\" WHERE \"Id\" = {report.Id}"));
                    Assert.Equal(d2.Id, host.Scalar($"SELECT \"DeptId\" FROM \"Employees\" WHERE \"Id\" = {other.Id}"));
                    Assert.Equal(manager.Id, host.Scalar($"SELECT \"ManagerId\" FROM \"Employees\" WHERE \"Id\" = {report.Id}"));
                    Assert.Equal(companyId, host.Scalar("SELECT \"CompanyId\" FROM \"Folders\""));
                    Assert.Equal(2, host.Scalar($"SELECT COUNT(*) FROM \"FileItems\" WHERE \"FolderId\" = (SELECT \"FolderId\" FROM \"FileItems\" LIMIT 1)"));
                    Assert.Equal(1, host.Scalar("SELECT COUNT(*) FROM \"Folders\""));
                    Assert.Empty(host.InsertsWithExplicitId("Companies", "Depts", "Employees", "FileItems"));

                    var c = payload.GetProperty("company");
                    Assert.Equal(companyId, c.GetProperty("id").GetInt32());
                    var depts = c.GetProperty("depts").EnumerateArray().ToList();
                    Assert.Equal(new[] { d1.Id, d2.Id }, depts.Select(d => d.GetProperty("id").GetInt32()));
                    Assert.All(depts, d => Assert.Equal(companyId, d.GetProperty("companyId").GetInt32()));
                    var emps = depts[0].GetProperty("employees").EnumerateArray().ToList();
                    Assert.Equal(new[] { manager.Id, report.Id }, emps.Select(e => e.GetProperty("id").GetInt32()));
                    Assert.All(emps, e => Assert.Equal(d1.Id, e.GetProperty("deptId").GetInt32()));
                    Assert.Equal(manager.Id, emps[1].GetProperty("managerId").GetInt32());
                    var f = Assert.Single(c.GetProperty("folders").EnumerateArray());
                    Assert.Equal(folder.Id, f.GetProperty("id").GetGuid());
                    Assert.Equal(companyId, f.GetProperty("companyId").GetInt32());
                    Assert.All(f.GetProperty("files").EnumerateArray(), x =>
                    {
                        Assert.Equal(folder.Id, x.GetProperty("folderId").GetGuid());
                        Assert.NotEqual(0, x.GetProperty("id").GetInt32());
                    });
                    Assert.All(folder.Files, x => Assert.Equal(folder.Id, x.FolderId));
                };
            }
            case GraphScenario.ManyToMany:
            {
                var existing = db.Tags.Single(t => t.Id == seed.TagId);
                var t1 = new Tag { Name = "t1" };
                var t2 = new Tag { Name = "t2" };
                var post = new Post { Title = "p", Tags = { t1, existing, t2 } };
                db.Posts.Add(post);
                outbox.AddEvent(new PostCreated { Post = post });

                return (host, payload) =>
                {
                    var postId = Assert.Single(host.Ints("SELECT \"Id\" FROM \"Posts\""));
                    Assert.Equal(postId, post.Id);
                    Assert.Equal(3, host.Scalar("SELECT COUNT(*) FROM \"Tags\""));
                    Assert.Equal(3, host.Scalar("SELECT COUNT(*) FROM \"PostTag\""));
                    Assert.Equal(
                        new[] { existing.Id, t1.Id, t2.Id }.OrderBy(x => x),
                        host.Ints($"SELECT \"TagsId\" FROM \"PostTag\" WHERE \"PostId\" = {postId} ORDER BY \"TagsId\""));
                    Assert.Empty(host.InsertsWithExplicitId("Posts", "Tags"));

                    var p = payload.GetProperty("post");
                    Assert.Equal(postId, p.GetProperty("id").GetInt32());
                    Assert.Equal(new[] { t1.Id, existing.Id, t2.Id }, p.GetProperty("tags").EnumerateArray().Select(t => t.GetProperty("id").GetInt32()));
                };
            }
            case GraphScenario.OwnedCollectionWithGeneratedKey:
            {
                var a = new Parcel { Label = "a" };
                var b = new Parcel { Label = "b" };
                var shipment = new Shipment { Ref = "s", Parcels = { a, b } };
                db.Shipments.Add(shipment);
                outbox.AddEvent(new ShipmentCreated { Shipment = shipment });

                return (host, payload) =>
                {
                    var shipmentId = Assert.Single(host.Ints("SELECT \"Id\" FROM \"Shipments\""));
                    Assert.Equal(shipmentId, shipment.Id);
                    Assert.Equal(new[] { a.Id, b.Id }, host.Ints($"SELECT \"Id\" FROM \"Parcels\" WHERE \"ShipmentId\" = {shipmentId} ORDER BY \"Id\""));
                    Assert.Equal(2, host.Scalar("SELECT COUNT(*) FROM \"Parcels\""));
                    Assert.Empty(host.InsertsWithExplicitId("Shipments", "Parcels"));

                    var s = payload.GetProperty("shipment");
                    Assert.Equal(shipmentId, s.GetProperty("id").GetInt32());
                    Assert.Equal(new[] { a.Id, b.Id }, s.GetProperty("parcels").EnumerateArray().Select(x => x.GetProperty("id").GetInt32()));
                };
            }
            case GraphScenario.AddedDependentOfUnchangedPrincipal:
            {
                var company = db.Companies.Include(c => c.Depts).Single(c => c.Id == seed.CompanyId);
                var employee = new Employee { Name = "new" };
                var dept = new Dept { Name = "new", Employees = { employee } };
                company.Depts.Add(dept);
                outbox.AddEvent(new CompanyCreated { Company = company });

                return (host, payload) =>
                {
                    Assert.Equal(4, host.Scalar("SELECT COUNT(*) FROM \"Companies\""));
                    Assert.Equal(2, host.Scalar("SELECT COUNT(*) FROM \"Depts\""));
                    Assert.Equal(seed.CompanyId, host.Scalar($"SELECT \"CompanyId\" FROM \"Depts\" WHERE \"Id\" = {dept.Id}"));
                    Assert.Equal(dept.Id, host.Scalar($"SELECT \"DeptId\" FROM \"Employees\" WHERE \"Id\" = {employee.Id}"));
                    Assert.NotEqual(seed.DeptId, dept.Id);
                    Assert.Equal(seed.CompanyId, dept.CompanyId);
                    Assert.Equal(0, host.InsertCount("Companies"));
                    Assert.Empty(host.InsertsWithExplicitId("Depts", "Employees"));

                    var depts = payload.GetProperty("company").GetProperty("depts").EnumerateArray().ToList();
                    var d = depts.Single(x => x.GetProperty("name").GetString() == "new");
                    Assert.Equal(dept.Id, d.GetProperty("id").GetInt32());
                    Assert.Equal(seed.CompanyId, d.GetProperty("companyId").GetInt32());
                    var e = Assert.Single(d.GetProperty("employees").EnumerateArray());
                    Assert.Equal(employee.Id, e.GetProperty("id").GetInt32());
                    Assert.Equal(dept.Id, e.GetProperty("deptId").GetInt32());
                };
            }
            case GraphScenario.ModifiedDependentReparentedToAddedPrincipal:
            {
                var company = db.Companies.Include(c => c.Depts).Single(c => c.Id == seed.CompanyId);
                var mover = db.Employees.Single(e => e.Id == seed.EmployeeId);
                db.Depts.Include(d => d.Employees).Single(d => d.Id == seed.DeptId).Employees.Remove(mover);
                var dept = new Dept { Name = "new" };
                dept.Employees.Add(mover);
                company.Depts.Add(dept);
                outbox.AddEvent(new CompanyCreated { Company = company });

                return (host, payload) =>
                {
                    Assert.Equal(2, host.Scalar("SELECT COUNT(*) FROM \"Depts\""));
                    Assert.Equal(dept.Id, host.Scalar($"SELECT \"DeptId\" FROM \"Employees\" WHERE \"Id\" = {seed.EmployeeId}"));
                    Assert.Equal(dept.Id, mover.DeptId);
                    Assert.Empty(host.InsertsWithExplicitId("Depts"));

                    var d = payload.GetProperty("company").GetProperty("depts").EnumerateArray().Single(x => x.GetProperty("name").GetString() == "new");
                    Assert.Equal(dept.Id, d.GetProperty("id").GetInt32());
                    Assert.Equal(dept.Id, Assert.Single(d.GetProperty("employees").EnumerateArray()).GetProperty("deptId").GetInt32());
                };
            }
            case GraphScenario.NotificationEntitiesWithoutBackingField:
            {
                var a = new NotifyItem { Label = "a" };
                var b = new NotifyItem { Label = "b" };
                db.NotifyItems.AddRange(a, b);
                outbox.AddEvent(new NotifyItemsCreated { Items = { a, b } });

                return (host, payload) =>
                {
                    var ids = host.Ints("SELECT \"Id\" FROM \"NotifyItems\" ORDER BY \"Id\"");
                    Assert.Equal(2, ids.Count);
                    Assert.Equal(ids.OrderBy(x => x), new[] { a.Id, b.Id }.OrderBy(x => x));
                    Assert.Empty(host.InsertsWithExplicitId("NotifyItems"));
                    Assert.Equal(new[] { a.Id, b.Id }, payload.GetProperty("items").EnumerateArray().Select(x => x.GetProperty("id").GetInt32()));
                };
            }
            case GraphScenario.SideEffectEntitiesWithoutBackingField:
            {
                var a = new SideEffectItem { Name = "a" };
                var b = new SideEffectItem { Name = "b" };
                db.SideEffectItems.AddRange(a, b);
                outbox.AddEvent(new SideEffectItemsCreated { Items = { a, b } });

                return (host, payload) =>
                {
                    var ids = host.Ints("SELECT \"Id\" FROM \"SideEffectItems\" ORDER BY \"Id\"");
                    Assert.Equal(2, ids.Count);
                    Assert.Equal(ids.OrderBy(x => x), new[] { a.Id, b.Id }.OrderBy(x => x));
                    Assert.Empty(host.InsertsWithExplicitId("SideEffectItems"));
                    Assert.Equal(new[] { a.Id, b.Id }, payload.GetProperty("items").EnumerateArray().Select(x => x.GetProperty("id").GetInt32()));
                };
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario));
        }
    }
}
