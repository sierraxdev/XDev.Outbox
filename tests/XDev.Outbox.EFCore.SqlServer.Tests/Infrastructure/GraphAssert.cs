using System.Text.Json;

namespace XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

public static class Graph
{
    public static User NewUser(string email, params string[] orderDescriptions)
    {
        var user = new User { Email = email };
        foreach (var description in orderDescriptions) user.Orders.Add(new Order { Description = description });
        return user;
    }
}

/// <summary>Rule A checks: payload keys equal the keys the database generated for the referenced rows.</summary>
public static class GraphAssert
{
    public static void UserPayloadMatches(string? payloadJson, IReadOnlyList<(int Id, string Email)> users, IReadOnlyList<(int Id, int UserId, string Description)> orders)
    {
        Assert.NotNull(payloadJson);
        var user = Json.Parse(payloadJson!).GetProperty("user");
        var email = user.GetProperty("email").GetString();
        var dbUser = Assert.Single(users, x => x.Email == email);
        Assert.Equal(dbUser.Id, user.GetProperty("id").GetInt32());

        if (user.TryGetProperty("orders", out var payloadOrders))
        {
            foreach (var order in payloadOrders.EnumerateArray())
            {
                var description = order.GetProperty("description").GetString();
                var dbOrder = Assert.Single(orders, x => x.Description == description);
                Assert.Equal(dbOrder.Id, order.GetProperty("id").GetInt32());
                Assert.Equal(dbOrder.UserId, order.GetProperty("userId").GetInt32());
                Assert.Equal(dbUser.Id, dbOrder.UserId);
            }
        }
    }

    public static void OrderPayloadMatches(string? payloadJson, IReadOnlyList<(int Id, int UserId, string Description)> orders)
    {
        Assert.NotNull(payloadJson);
        var order = Json.Parse(payloadJson!).GetProperty("order");
        var description = order.GetProperty("description").GetString();
        var dbOrder = Assert.Single(orders, x => x.Description == description);
        Assert.Equal(dbOrder.Id, order.GetProperty("id").GetInt32());
        Assert.Equal(dbOrder.UserId, order.GetProperty("userId").GetInt32());
    }

    /// <summary>Checks every row of a UserCreated/OrderPlaced mix against the database.</summary>
    public static void RowsMatch(IEnumerable<OutboxRow> rows, IReadOnlyList<(int Id, string Email)> users, IReadOnlyList<(int Id, int UserId, string Description)> orders)
    {
        foreach (var row in rows)
        {
            if (row.Type == typeof(UserCreated).AssemblyQualifiedName) UserPayloadMatches(row.Payload, users, orders);
            else if (row.Type == typeof(OrderPlaced).AssemblyQualifiedName) OrderPayloadMatches(row.Payload, orders);
            else Assert.Fail("Unexpected outbox row type: " + row.Type);
        }
    }

    public static bool JsonEquals(string? left, string? right)
    {
        if (left == null || right == null) return left == right;
        return JsonElementEquals(Json.Parse(left), Json.Parse(right));
    }

    private static bool JsonElementEquals(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind) return false;
        switch (a.ValueKind)
        {
            case JsonValueKind.Object:
                var pa = a.EnumerateObject().OrderBy(x => x.Name).ToList();
                var pb = b.EnumerateObject().OrderBy(x => x.Name).ToList();
                return pa.Count == pb.Count && pa.Zip(pb).All(x => x.First.Name == x.Second.Name && JsonElementEquals(x.First.Value, x.Second.Value));
            case JsonValueKind.Array:
                var ea = a.EnumerateArray().ToList();
                var eb = b.EnumerateArray().ToList();
                return ea.Count == eb.Count && ea.Zip(eb).All(x => JsonElementEquals(x.First, x.Second));
            case JsonValueKind.String:
                return a.GetString() == b.GetString();
            default:
                return a.GetRawText() == b.GetRawText();
        }
    }
}
