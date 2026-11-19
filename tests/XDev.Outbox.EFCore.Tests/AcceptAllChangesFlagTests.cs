using System.Dynamic;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// #2 / D3: with acceptAllChangesOnSuccess = false and a library-owned transaction, the caller's flag is honored
/// (entries stay as they were) and the event is still persisted and dispatched once. The dispatched payload is
/// rebuilt from the persisted JSON (D3), not the original instance.
/// </summary>
public class AcceptAllChangesFlagTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSave_WithAcceptFalse_HonorsFlagPersistsAndDispatchesRebuiltPayloadOnce(bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();

        var user = new User { Email = "a@test" };
        scope.Db.Users.Add(user);
        var evt = new UserCreated { User = user };
        scope.Outbox.AddEvent(evt);

        await Save.Invoke(scope.Db, async, acceptAllChangesOnSuccess: false);

        // Flag honored: nothing accepted, no outbox entity left tracked.
        Assert.Equal(EntityState.Added, scope.Db.Entry(user).State);
        Assert.Equal(0, scope.TrackedOutboxMessages);

        // Persisted once, with the real key.
        var userId = Assert.Single(host.UserIds());
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(userId, Payload.User(row.Payload).GetProperty("id").GetInt32());

        // Dispatched once; payload rebuilt from JSON (D3) and carries the real key.
        var enqueued = Assert.Single(host.Queue.Enqueued);
        Assert.Equal(row.Id, enqueued.Id);
        Assert.NotNull(enqueued.Payload);
        Assert.NotSame(evt, enqueued.Payload);
        Assert.Equal(userId, Payload.User(Payload.ToJson(enqueued.Payload)).GetProperty("id").GetInt32());

        // The dispatcher (OutboxProcessorHostedService) must be able to turn it into the event type.
        var dispatched = Assert.IsType<UserCreated>(ConvertLikeDispatcher(enqueued.Payload!, enqueued.Type!));
        Assert.Equal(userId, dispatched.User!.Id);

        Assert.Empty(scope.Outbox.GetEvents());

        // The caller then accepts and saves again: no duplicate business row, outbox row or dispatch.
        scope.Db.ChangeTracker.AcceptAllChanges();
        Assert.Equal(EntityState.Unchanged, scope.Db.Entry(user).State);
        Assert.Equal(userId, user.Id);

        await Save.Invoke(scope.Db, async);

        Assert.Single(host.UserIds());
        Assert.Single(host.OutboxRows());
        Assert.Single(host.Queue.Enqueued);
    }

    /// <summary>Mirrors the payload conversion in OutboxProcessorHostedService.HandleMessageAsync.</summary>
    private static object? ConvertLikeDispatcher(object payload, string typeName)
    {
        var type = Type.GetType(typeName) ?? throw new InvalidOperationException("Type not loadable: " + typeName);
        return payload switch
        {
            ExpandoObject expando => expando.ToObject(type),
            JsonElement element => element.ToObject(type),
            _ => payload,
        };
    }
}
