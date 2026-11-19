using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests;

/// <summary>
/// The poller's store calls (RemoveAsync / UpdateAsync) save through the OutboxDbContext override. With no
/// buffered events they must behave as plain saves: change the targeted row and dispatch nothing.
/// </summary>
public class OutboxStorePassThroughTests
{
    [Fact]
    public async Task RemoveAsync_OnPersistedMessage_DeletesRowAndEnqueuesNothing()
    {
        using var host = OutboxTestHost.Create();
        var keepId = host.SeedOutboxRow(typeof(UserCreated).AssemblyQualifiedName!, "{\"user\":{\"id\":1}}");
        var removeId = host.SeedOutboxRow(typeof(UserCreated).AssemblyQualifiedName!, "{\"user\":{\"id\":2}}");

        using var scope = host.CreateScope();
        var store = scope.Services.GetRequiredService<IOutboxStore<int>>();
        var message = scope.Db.OutboxMessages.AsNoTracking().Single(x => x.Id == removeId);

        await store.RemoveAsync(message);

        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(keepId, row.Id);
        Assert.Empty(host.Queue.Enqueued);
    }

    [Fact]
    public async Task UpdateAsync_OnPersistedMessage_UpdatesFailureColumnsAndEnqueuesNothing()
    {
        using var host = OutboxTestHost.Create();
        var id = host.SeedOutboxRow(typeof(UserCreated).AssemblyQualifiedName!, "{\"user\":{\"id\":1}}");

        using var scope = host.CreateScope();
        var store = scope.Services.GetRequiredService<IOutboxStore<int>>();
        var message = scope.Db.OutboxMessages.AsNoTracking().Single(x => x.Id == id);
        message.Attemps = 1;
        message.IsError = true;
        message.ErrorMessage = "boom";

        await store.UpdateAsync(message);

        Assert.Equal((1, true, "boom"), host.ReadOutboxFailureColumns(id));
        var row = Assert.Single(host.OutboxRows());
        Assert.Equal(1, Payload.User(row.Payload).GetProperty("id").GetInt32());
        Assert.Empty(host.Queue.Enqueued);
    }
}
