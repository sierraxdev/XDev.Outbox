using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Loader;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace XDev.Outbox.Tests;

/// <summary>
/// The hosted service resolves a stored type name whose assembly is not found by name (Type.GetType alone fails) from
/// the assemblies already loaded: a dynamic assembly, and a plugin assembly loaded after a failed lookup by simple name
/// (the runtime remembers that failure, so Type.GetType never resolves it until restart). Assembly names are unique per
/// test (a failed lookup is remembered for the whole test process).
/// </summary>
public class TypeNameLoadedAssemblyResolutionTests
{
    private static async Task<(Exception? Error, RecordingStore Store)> Deliver(string typeName)
    {
        var store = new RecordingStore();
        var queue = new CapturingQueue();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IMessageQueue<int>>(queue);
        services.AddScoped<IOutboxStore<int>>(_ => store);
        services.AddOutbox(typeof(TypeNameLoadedAssemblyResolutionTests).Assembly);
        services.AddSingleton<InvocationLog>();
        services.AddSingleton<ThrownExceptions>();
        using var provider = services.BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().OfType<OutboxProcessorHostedService<int>>().Single();
        using var cts = new CancellationTokenSource();
        await hosted.StartAsync(cts.Token);
        try
        {
            for (var i = 0; queue.Handler == null && i < 200; i++) await Task.Delay(10);
            Assert.NotNull(queue.Handler);

            using var json = JsonDocument.Parse("{}");
            var message = new OutboxMessage<int>
            {
                Id = 21,
                Type = typeName,
                Payload = json.RootElement.Clone(),
                Created = DateTime.UtcNow,
            };
            var error = await Record.ExceptionAsync(() => queue.Handler!(message, CancellationToken.None));
            return (error, store);
        }
        finally
        {
            cts.Cancel();
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    private static void AssertDelivered((Exception? Error, RecordingStore Store) result)
    {
        Assert.Null(result.Error);
        Assert.Equal(21, Assert.Single(result.Store.Removed).Id);
        Assert.Empty(result.Store.Updated);
    }

    private static void AssertFailureRecorded((Exception? Error, RecordingStore Store) result)
    {
        Assert.NotNull(result.Error);
        Assert.Contains("Failed to load type", result.Error!.Message);
        var updated = Assert.Single(result.Store.Updated);
        Assert.Equal(1, updated.Attemps);
        Assert.True(updated.IsError);
        Assert.Empty(result.Store.Removed);
    }

    private static TypeBuilder DefineEventType(ModuleBuilder module)
    {
        var type = module.DefineType("XDevPlugin.Events.PluginEvent", TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed, typeof(object));
        type.DefineDefaultConstructor(MethodAttributes.Public);
        return type;
    }

    [Fact]
    public async Task HandleMessage_TypeInADynamicAssemblyNotFoundByName_IsResolvedFromTheLoadedAssemblies()
    {
        var assemblyName = "XDevDynamicEvents_" + Guid.NewGuid().ToString("N");
        var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(assemblyName), AssemblyBuilderAccess.Run);
        var type = DefineEventType(assembly.DefineDynamicModule(assemblyName)).CreateType();
        Assert.Null(Type.GetType($"{type.FullName}, {assemblyName}")); // Precondition: not found by name.

        AssertDelivered(await Deliver($"{type.FullName}, {assemblyName}"));
        AssertDelivered(await Deliver(type.AssemblyQualifiedName!));
    }

    [Fact]
    public async Task HandleMessage_FullNameRequestingALowerVersionThanTheLoadedAssembly_IsResolvedFromTheLoadedAssemblies()
    {
        var assemblyName = "XDevDynamicEvents_" + Guid.NewGuid().ToString("N");
        var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(assemblyName) { Version = new Version(2, 0, 0, 0) }, AssemblyBuilderAccess.Run);
        var type = DefineEventType(assembly.DefineDynamicModule(assemblyName)).CreateType();
        var lower = $"{type.FullName}, {assemblyName}, Version=1.5.0.0, Culture=neutral, PublicKeyToken=null";
        Assert.Null(Type.GetType(lower)); // Precondition: not found by name.

        AssertDelivered(await Deliver(lower));
    }

    [Fact]
    public async Task HandleMessage_FullNameRequestingAHigherVersionThanTheLoadedAssembly_RecordsTheFailure()
    {
        var assemblyName = "XDevDynamicEvents_" + Guid.NewGuid().ToString("N");
        var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(assemblyName) { Version = new Version(2, 0, 0, 0) }, AssemblyBuilderAccess.Run);
        var type = DefineEventType(assembly.DefineDynamicModule(assemblyName)).CreateType();

        AssertFailureRecorded(await Deliver($"{type.FullName}, {assemblyName}, Version=2.0.0.1, Culture=neutral, PublicKeyToken=null"));
        AssertFailureRecorded(await Deliver($"{type.FullName}, {assemblyName}, Version=3.0.0.0, Culture=neutral, PublicKeyToken=null"));
        AssertDelivered(await Deliver($"{type.FullName}, {assemblyName}, Version=2.0.0.0, Culture=neutral, PublicKeyToken=null")); // Control.
    }

    [Fact]
    public async Task HandleMessage_UnknownAssemblyOrUnknownTypeInAKnownAssembly_RecordsTheFailure()
    {
        AssertFailureRecorded(await Deliver("Missing.Namespace.MissingEvent, Missing.Assembly_" + Guid.NewGuid().ToString("N")));
        AssertFailureRecorded(await Deliver("Missing.Namespace.MissingEvent, XDev.Outbox.Tests"));
    }

#if NET9_0_OR_GREATER
    /// <summary>A plugin assembly (on disk in a real application) as bytes, never loadable by name from the app folder.</summary>
    private static (byte[] Image, string AssemblyName, string TypeName) BuildPlugin()
    {
        var assemblyName = "XDevPluginEvents_" + Guid.NewGuid().ToString("N");
        var builder = new PersistedAssemblyBuilder(new AssemblyName(assemblyName), typeof(object).Assembly);
        var type = DefineEventType(builder.DefineDynamicModule(assemblyName));
        type.CreateType();
        using var image = new MemoryStream();
        builder.Save(image);
        return (image.ToArray(), assemblyName, "XDevPlugin.Events.PluginEvent");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandleMessage_PluginLoadedAfterAFailedLookupBySimpleName_ResolvesOnceLoaded(bool defaultContext)
    {
        var (image, assemblyName, typeName) = BuildPlugin();
        var versionLess = $"{typeName}, {assemblyName}";

        // The message arrives before the plugin is loaded: the lookup by simple name fails and the runtime remembers it.
        AssertFailureRecorded(await Deliver(versionLess));

        var context = defaultContext ? AssemblyLoadContext.Default : new AssemblyLoadContext(assemblyName);
        var plugin = context.LoadFromStream(new MemoryStream(image));
        if (defaultContext)
        {
            // What the hosted service used before: still not found, although the assembly is now loaded.
            Assert.Null(Type.GetType(versionLess));
        }

        AssertDelivered(await Deliver(versionLess));
        AssertDelivered(await Deliver(plugin.GetType(typeName, throwOnError: true)!.AssemblyQualifiedName!));
    }
#endif
}
