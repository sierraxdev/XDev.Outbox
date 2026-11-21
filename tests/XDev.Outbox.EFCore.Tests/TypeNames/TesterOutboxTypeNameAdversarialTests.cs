using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Loader;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.TypeNames;

// Task fix-claim-column-names-and-long-type-names (Bug 5), adversarial tester (2026-11-20).

public struct TesterGenericStruct<T>
{
    public T Value { get; set; }
}

public class TesterOuter<T>
{
    public class Mid<U>
    {
        public class Inner<V>
        {
            public V? Value { get; set; }
        }

        public class Plain
        {
            public U? Value { get; set; }
        }
    }
}

/// <summary>
/// Dynamic assemblies (never on disk) resolvable through AppDomain.AssemblyResolve, which plain Type.GetType(string) — the
/// hosted service's call — consults for assemblies the default context cannot bind.
/// </summary>
internal static class DynamicEventAssemblies
{
    private static readonly ConcurrentDictionary<string, AssemblyBuilder> Assemblies = new();
    private static int _registered;

    public static AssemblyBuilder Get(string simpleName, bool resolvable)
    {
        if (resolvable && Interlocked.Exchange(ref _registered, 1) == 0)
        {
            AppDomain.CurrentDomain.AssemblyResolve += (_, e) =>
            {
                var name = new AssemblyName(e.Name).Name;
                return name != null && Assemblies.TryGetValue(name, out var assembly) ? assembly : null;
            };
        }

        var builder = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName { Name = simpleName }, AssemblyBuilderAccess.Run);
        if (resolvable) Assert.True(Assemblies.TryAdd(simpleName, builder), "Unique dynamic assembly names per test.");
        return builder;
    }

    private static readonly DynamicModules Modules = new();

    public static Type DefineType(AssemblyBuilder assembly, string name)
    {
        lock (Modules)
        {
            var module = Modules.GetOrAdd(assembly);
            return module.DefineType(name, TypeAttributes.Public | TypeAttributes.Class).CreateType()!;
        }
    }

    /// <summary>One module per dynamic assembly (the runtime allows only one).</summary>
    private sealed class DynamicModules
    {
        private readonly Dictionary<AssemblyBuilder, ModuleBuilder> _modules = new();

        public ModuleBuilder GetOrAdd(AssemblyBuilder assembly)
        {
            if (!_modules.TryGetValue(assembly, out var module))
            {
                module = assembly.DefineDynamicModule("m");
                _modules[assembly] = module;
            }
            return module;
        }
    }
}

public class TesterOutboxTypeNameAdversarialTests
{
    private readonly ITestOutputHelper _output;

    public TesterOutboxTypeNameAdversarialTests(ITestOutputHelper output) => _output = output;

    /// <summary>Independent oracle: the full name with every ", Version=..., Culture=..., PublicKeyToken=..." removed.</summary>
    private static string StripVersionCultureKey(string assemblyQualifiedName)
        => Regex.Replace(assemblyQualifiedName, @", Version=[^,\]]*, Culture=[^,\]]*, PublicKeyToken=[^,\]]*", "");

    private static string Unique(string prefix) => prefix + "_" + Guid.NewGuid().ToString("N").Substring(0, 8);

    public static TheoryData<Type> OddShapes() => new()
    {
        typeof((int, string, Guid, long, byte, short, char, decimal, DateTime)), // ValueTuple`8 with TRest
        typeof((int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int)), // TRest of TRest
        typeof(TesterGenericStruct<List<int>>?),
        typeof(TesterGenericStruct<TesterGenericStruct<Guid?>>?[]),
        typeof(TesterOuter<int>.Mid<string>.Inner<Guid>),
        typeof(TesterOuter<User>.Mid<Order[]>.Plain[]),
        typeof(TesterOuter<int>.Mid<string>.Inner<Guid>[][,]),
        typeof(TesterOuter<TesterOuter<int>.Mid<User>.Plain>.Mid<GenericOuter<Guid>.InnerGeneric<User>>.Inner<EntityChanged<User>[]>),
        typeof(Dictionary<EntityChanged<User>[], GenericOuter<int>.InnerGeneric<Guid?>>),
        typeof(EntityChanged<User>[,][]),
        typeof(KeyValuePair<(int, string), List<Dictionary<Guid, User[]>>>),
        typeof(Func<int, Task<User>>),
        typeof(Action<UserCreated>),
    };

    [Theory]
    [MemberData(nameof(OddShapes))]
    public void Get_OddShapes_EqualsTheFullNameWithoutVersionCultureAndKeyAndResolvesToTheSameType(Type type)
    {
        var name = OutboxTypeName.Get(type);
        _output.WriteLine($"{name.Length}/{type.AssemblyQualifiedName!.Length}: {name}");

        Assert.Equal(StripVersionCultureKey(type.AssemblyQualifiedName!), name);
        Assert.Same(type, Type.GetType(name, throwOnError: true));
    }

    [Theory]
    [MemberData(nameof(OutboxTypeNameTests.ResolvableTypes), MemberType = typeof(OutboxTypeNameTests))]
    public void Get_DeveloperTypes_EqualTheFullNameWithoutVersionCultureAndKey(Type type)
        => Assert.Equal(StripVersionCultureKey(type.AssemblyQualifiedName!), OutboxTypeName.Get(type));

    public static TheoryData<string, string> OddDynamicNames()
    {
        var data = new TheoryData<string, string>();
        foreach (var assembly in new[] { "Plain", "Odd,Comma", "Odd=Equals", "Odd[Open", "Odd\"Quote'", "Odd+Plus", "Odd&Amp*Star", "Odd Space", "Odd😀Emoji" })
        foreach (var type in new[] { "Ns.Plain", "Ns.Weird,Comma", "Ns.Br[ack]et", "Ns.Plus+Sign", "Ns.Amp&Star*", @"Ns.Back\slash", "Ns.Eq=Sign", "Ns.Emoji😀Ü" })
            data.Add(assembly, type);
        return data;
    }

    /// <summary>
    /// Escaping: special characters in the simple assembly name and in the type name (escaped by Type.FullName) survive,
    /// for the type itself, as a generic argument and inside an array of a generic. Plain Type.GetType resolves the stored
    /// name exactly like the full name.
    /// </summary>
    [Theory]
    [MemberData(nameof(OddDynamicNames))]
    public void Get_SpecialCharactersInAssemblyAndTypeNames_ResolveWithPlainTypeGetTypeLikeTheFullName(string assemblyName, string typeName)
    {
        var assembly = DynamicEventAssemblies.Get(Unique(assemblyName), resolvable: true);
        var type = DynamicEventAssemblies.DefineType(assembly, typeName);

        foreach (var shape in new[] { type, typeof(List<>).MakeGenericType(type), typeof(Dictionary<,>).MakeGenericType(type, typeof(Guid)).MakeArrayType(), type.MakeArrayType(2) })
        {
            var name = OutboxTypeName.Get(shape);
            _output.WriteLine(name);
            Assert.Same(shape, Type.GetType(shape.AssemblyQualifiedName!, throwOnError: true)); // control: the full name resolves
            Assert.Same(shape, Type.GetType(name, throwOnError: false));
            // A quoted assembly name is spelled "..." by AssemblyName without a version and '...' with one; both parse.
            if (!assemblyName.Contains('"')) Assert.Equal(StripVersionCultureKey(shape.AssemblyQualifiedName!), name);
        }
    }

    /// <summary>
    /// A simple assembly name containing ']' (legal; AssemblyName does not escape it). On net8 an unescaped ']' in a
    /// generic argument ends the bracketed argument early (the tester's low-severity regression); OutboxTypeName escapes
    /// it there ("\]", as AssemblyQualifiedName does), so every shape resolves like the full name. On net9+ the type-name
    /// parser rejects the ']' in either form, for every shape (parity: neither resolves).
    /// </summary>
    [Fact]
    public void AssemblyNameWithClosingBracket_Net8EveryFormResolves_Net9PlusNeitherFormResolves()
    {
        var assembly = DynamicEventAssemblies.Get(Unique("Odd]Close"), resolvable: true);
        var type = DynamicEventAssemblies.DefineType(assembly, "Ns.Evt");
        var list = typeof(List<>).MakeGenericType(type);
        // net9+ may throw FileLoadException ("The given assembly name was invalid") even with throwOnError: false; the
        // hosted service then records a failed attempt, as for a null result.
        static bool Resolves(string name, Type t)
        {
            try { return Type.GetType(name, throwOnError: false) == t; }
            catch (FileLoadException) { return false; }
        }
        bool Short(Type t) => Resolves(OutboxTypeName.Get(t), t);
        bool Full(Type t) => Resolves(t.AssemblyQualifiedName!, t);
        _output.WriteLine(OutboxTypeName.Get(list));

        if (Environment.Version.Major == 8)
        {
            Assert.True(Short(type) && Full(type));
            Assert.True(Short(type.MakeArrayType()) && Full(type.MakeArrayType()));
            Assert.True(Full(list));
            Assert.True(Short(list)); // fixed: the ']' is escaped inside the generic argument
            Assert.Contains(@"\]", OutboxTypeName.Get(list));
            Assert.DoesNotContain(@"\]", OutboxTypeName.Get(type)); // not at the top level (does not load there)
            foreach (var shape in new[] { typeof(Dictionary<,>).MakeGenericType(type, typeof(Guid)).MakeArrayType(), typeof(List<>).MakeGenericType(list), list.MakeArrayType(2) })
            {
                Assert.True(Full(shape), shape.ToString());
                Assert.True(Short(shape), shape.ToString());
            }
        }
        else
        {
            foreach (var shape in new[] { type, type.MakeArrayType(), list })
            {
                Assert.False(Short(shape));
                Assert.False(Full(shape));
            }
        }
    }

    /// <summary>Parity: a dynamic assembly nobody resolves is not found by name, with either form.</summary>
    [Fact]
    public void Get_DynamicAssemblyWithoutResolver_NeitherTheStoredNorTheFullNameResolves()
    {
        var assembly = DynamicEventAssemblies.Get(Unique("Unresolvable"), resolvable: false);
        var type = DynamicEventAssemblies.DefineType(assembly, "Ns.Evt");
        foreach (var shape in new[] { type, typeof(List<>).MakeGenericType(type) })
        {
            Assert.Null(Type.GetType(OutboxTypeName.Get(shape), throwOnError: false));
            Assert.Null(Type.GetType(shape.AssemblyQualifiedName!, throwOnError: false));
        }
    }

    /// <summary>
    /// Parity: a second copy of an application assembly in a collectible AssemblyLoadContext has the same names; both the
    /// stored and the full name resolve to the default context's type (never to the plugin copy).
    /// </summary>
    [Fact]
    public void Get_TypeFromACollectibleLoadContextCopyOfTheAssembly_BothNamesResolveToTheDefaultContextType()
    {
        var context = new AssemblyLoadContext(Unique("tester-plugin"), isCollectible: true);
        try
        {
            var copy = context.LoadFromAssemblyPath(typeof(OuterEvents).Assembly.Location);
            var pluginType = copy.GetType(typeof(OuterEvents.Nested).FullName!, throwOnError: true)!;
            var pluginList = typeof(List<>).MakeGenericType(pluginType);
            Assert.NotSame(typeof(OuterEvents.Nested), pluginType);

            Assert.Equal(OutboxTypeName.Get(typeof(OuterEvents.Nested)), OutboxTypeName.Get(pluginType));
            Assert.Same(typeof(OuterEvents.Nested), Type.GetType(OutboxTypeName.Get(pluginType), throwOnError: true));
            Assert.Same(typeof(OuterEvents.Nested), Type.GetType(pluginType.AssemblyQualifiedName!, throwOnError: true));
            Assert.Same(typeof(List<OuterEvents.Nested>), Type.GetType(OutboxTypeName.Get(pluginList), throwOnError: true));
            Assert.Same(typeof(List<OuterEvents.Nested>), Type.GetType(pluginList.AssemblyQualifiedName!, throwOnError: true));
        }
        finally
        {
            context.Unload();
        }
    }
}

/// <summary>The length check at its boundaries (SQLite, which ignores the column length: only the check stops a long name).</summary>
public class TesterEventTypeNameLengthBoundaryTests
{
    private static readonly AssemblyBuilder Assembly = DynamicEventAssemblies.Get("XDevTesterLen", resolvable: false);
    private static readonly object Gate = new();
    private static readonly Dictionary<string, Type> Types = new();

    /// <summary>A type whose stored name is exactly <paramref name="length"/> UTF-16 units, with surrogate pairs.</summary>
    private static Type OfLength(int length)
    {
        const string suffix = ", XDevTesterLen";
        var name = ("Tester.Ev\U0001F600\U0001D49C_" + length + "_").PadRight(length - suffix.Length, 'x');
        lock (Gate)
        {
            if (!Types.TryGetValue(name, out var type))
            {
                type = DynamicEventAssemblies.DefineType(Assembly, name);
                Types[name] = type;
            }
            Assert.Equal(length, OutboxTypeName.Get(type).Length);
            return type;
        }
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(199, false)]
    [InlineData(200, false)]
    [InlineData(200, true)]
    public async Task Save_StoredNameAtMostTheDefaultMaxLength_IsSaved(int length, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var type = length < 20 ? typeof(UserCreated) : OfLength(length);
        scope.Outbox.AddEvent(Activator.CreateInstance(type)!);

        await Save.Invoke(scope.Db, async);

        Assert.Equal(OutboxTypeName.Get(type), Assert.Single(host.OutboxRows()).Type);
    }

    [Theory]
    [InlineData(201, false)]
    [InlineData(201, true)]
    [InlineData(1000, false)]
    public async Task Save_StoredNameOverTheDefaultMaxLength_Throws(int length, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        scope.Outbox.AddEvent(Activator.CreateInstance(OfLength(length))!);
        host.Faults.ExecutedCommands.Clear();

        var error = await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async));

        Assert.Contains($"{length} characters", Assert.IsType<InvalidOperationException>(error).Message);
        Assert.Empty(host.Faults.ExecutedCommands);
        Assert.Equal(0, host.Count("OutboxMessages"));
    }

    [Fact]
    public async Task Save_SameTooLongTypeManyTimes_ThrowsOnceNamingItAndKeepsEveryEvent()
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var type = OfLength(201);
        var events = Enumerable.Range(0, 1000).Select(_ => Activator.CreateInstance(type)!).ToArray();
        scope.Outbox.AddEvent(events);

        var error = await Record.ExceptionAsync(() => scope.Db.SaveChangesAsync());

        Assert.IsType<InvalidOperationException>(error);
        Assert.Equal(1000, scope.Outbox.GetEvents().Count);
        Assert.Equal(0, host.Count("OutboxMessages"));
    }

    [Fact]
    public async Task Save_AfterTheRejectedSaveTheContextStillSavesOnceTheEventIsRemoved()
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        var user = new User { Email = "again@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(Activator.CreateInstance(OfLength(201))!);
        Assert.IsType<InvalidOperationException>(await Record.ExceptionAsync(() => scope.Db.SaveChangesAsync()));

        // Repeated saves keep failing the same way (nothing half-done in between).
        Assert.IsType<InvalidOperationException>(await Record.ExceptionAsync(() => scope.Db.SaveChangesAsync()));
        Assert.Equal(0, host.Count("Users"));

        scope.Outbox.ClearEvents();
        scope.Outbox.AddEvent(new UserCreated { User = user });
        await scope.Db.SaveChangesAsync();
        Assert.Equal(1, host.Count("Users"));
        Assert.Equal(OutboxTypeName.Get(typeof(UserCreated)), Assert.Single(host.OutboxRows()).Type);
        Assert.Single(host.Queue.Enqueued);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Save_TooLongEventAddedBySavingChangesHandlerDuringTheSave_SaveSucceedsAndTheNextSaveRejectsIt(bool callerTransaction, bool async)
    {
        using var host = OutboxTestHost.Create();
        using var scope = host.CreateScope();
        object? during = null;
        scope.Db.SavingChanges += (_, _) =>
        {
            if (during == null) scope.Outbox.AddEvent(during = Activator.CreateInstance(OfLength(201))!);
        };
        var user = new User { Email = "during@test" };
        scope.Db.Users.Add(user);
        scope.Outbox.AddEvent(new UserCreated { User = user });

        var tx = callerTransaction ? scope.Db.Database.BeginTransaction() : null;
        await Save.Invoke(scope.Db, async);
        tx?.Commit();
        tx?.Dispose();

        Assert.NotNull(during);
        Assert.Equal(OutboxTypeName.Get(typeof(UserCreated)), Assert.Single(host.OutboxRows()).Type);
        Assert.Same(during, Assert.Single(scope.Outbox.GetEvents()));

        host.Faults.ExecutedCommands.Clear();
        scope.Db.Users.Add(new User { Email = "next@test" });
        Assert.IsType<InvalidOperationException>(await Record.ExceptionAsync(() => Save.Invoke(scope.Db, async)));
        Assert.Empty(host.Faults.ExecutedCommands);
        Assert.Single(host.OutboxRows());
        Assert.Equal(1, host.Count("Users"));
        Assert.Single(host.Queue.Enqueued);
    }
}
