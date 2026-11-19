using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// Rule A (D1): when an event references a business entity, the persisted payload carries the entity's real
/// store-generated key and FKs, whichever overload, flag or transaction ownership is used.
/// </summary>
public class PayloadKeyTests
{
    /// <summary>Overload under test: "SaveChanges()", "SaveChanges(true)", "SaveChangesAsync(false)", ...</summary>
    public static TheoryData<string, bool> Combinations()
    {
        var data = new TheoryData<string, bool>();
        foreach (var overload in new[]
                 {
                     "SaveChanges()", "SaveChanges(true)", "SaveChanges(false)",
                     "SaveChangesAsync()", "SaveChangesAsync(true)", "SaveChangesAsync(false)",
                 })
        foreach (var ambient in new[] { false, true })
            data.Add(overload, ambient);
        return data;
    }

    [Theory]
    [MemberData(nameof(Combinations))]
    public async Task Save_EventReferencingNewUserAndOrder_PersistsPayloadWithGeneratedKeys(string overload, bool ambientTransaction)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        // Seed one user so the new user's generated id is not 1 and cannot match by accident.
        using (var cmd = host.Connection.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO \"Users\" (\"Email\") VALUES ('seed@test')";
            cmd.ExecuteNonQuery();
        }

        var user = new User { Email = "new@test" };
        user.Orders.Add(new Order { Description = "o" });
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        var async = overload.StartsWith("SaveChangesAsync", StringComparison.Ordinal);
        if (ambientTransaction)
        {
            using var tx = async
                ? await scope.Db.Database.BeginTransactionAsync()
                : scope.Db.Database.BeginTransaction();
            await Invoke(scope.Db, overload);
            if (async) await tx.CommitAsync(); else tx.Commit();
        }
        else
        {
            await Invoke(scope.Db, overload);
        }

        var userIds = host.UserIds();
        Assert.Equal(2, userIds.Count);
        var newUserId = userIds.Max();
        var orderId = Assert.Single(host.OrderIds());

        var row = Assert.Single(host.OutboxRows());
        var payloadUser = Payload.User(row.Payload);
        Assert.Equal(newUserId, payloadUser.GetProperty("id").GetInt32());
        var payloadOrder = Assert.Single(payloadUser.GetProperty("orders").EnumerateArray());
        Assert.Equal(orderId, payloadOrder.GetProperty("id").GetInt32());
        Assert.Equal(newUserId, payloadOrder.GetProperty("userId").GetInt32());
    }

    private static async Task Invoke(DbContext db, string overload)
    {
        switch (overload)
        {
            case "SaveChanges()": db.SaveChanges(); break;
            case "SaveChanges(true)": db.SaveChanges(true); break;
            case "SaveChanges(false)": db.SaveChanges(false); break;
            case "SaveChangesAsync()": await db.SaveChangesAsync(); break;
            case "SaveChangesAsync(true)": await db.SaveChangesAsync(true); break;
            case "SaveChangesAsync(false)": await db.SaveChangesAsync(false); break;
            default: throw new ArgumentOutOfRangeException(nameof(overload), overload, null);
        }
    }
}
