using System.Collections.ObjectModel;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;
using XDev.Outbox.EFCore.Tests.Infrastructure;

namespace XDev.Outbox.EFCore.Tests.TypeNames;

// Event shapes used by the type-name tests. AddOutbox scans the assembly, but none of these has a subscriber.

public class EntityChanged<T>
{
    public T? Entity { get; set; }
    public string? Change { get; set; }
}

public class EntityChanged<T1, T2>
{
    public T1? First { get; set; }
    public T2? Second { get; set; }
}

public class OuterEvents
{
    public class Nested
    {
        public string? Text { get; set; }
    }
}

public class GenericOuter<T>
{
    public class Inner
    {
        public T? Value { get; set; }
    }

    public class InnerGeneric<TInner>
    {
        public TInner? Value { get; set; }
    }
}

/// <summary>
/// The stored event type name (internal <c>OutboxTypeName</c>): no Version, Culture or PublicKeyToken at any level,
/// and <see cref="Type.GetType(string)"/> resolves it to the same type on every target framework.
/// </summary>
public class OutboxTypeNameTests
{
    private readonly ITestOutputHelper _output;

    public OutboxTypeNameTests(ITestOutputHelper output) => _output = output;

    public static TheoryData<Type> ResolvableTypes() => new()
    {
        // Plain, nested and generic types of the test assembly.
        typeof(UserCreated),
        typeof(OuterEvents.Nested),
        typeof(GenericOuter<User>.Inner),
        typeof(GenericOuter<User>.InnerGeneric<Guid>),
        typeof(EntityChanged<User>),
        typeof(EntityChanged<User, Guid>),
        typeof(EntityChanged<User, Order>),
        typeof(EntityChanged<EntityChanged<User>>),
        // Generic arguments from another assembly of the application (XDev.Outbox) and from System.Private.CoreLib.
        typeof(EntityChanged<OutboxMessage<int>>),
        typeof(EntityChanged<Guid>),
        typeof(EntityChanged<Guid?>),
        typeof(EntityChanged<List<Guid>>),
        typeof(EntityChanged<Dictionary<string, List<User>>>),
        typeof(EntityChanged<(int, string)>),
        typeof(EntityChanged<KeyValuePair<int, Guid>>),
        typeof(List<User>),
        typeof(Dictionary<string, Guid>),
        // Strong-named framework assemblies outside CoreLib (System.Text.Json, System.ObjectModel) and strong-named
        // package assemblies loaded from the application folder (EF Core, Microsoft.Data.Sqlite).
        typeof(EntityChanged<JsonElement>),
        typeof(EntityChanged<ReadOnlyObservableCollection<User>>),
        typeof(EntityChanged<EntityState>),
        typeof(EntityChanged<SqliteConnectionStringBuilder>),
        typeof(JsonElement),
        typeof(EntityChanged<JsonElement, List<SqliteOpenMode>>),
        // Arrays: single-dimensional, jagged, multi-dimensional, rank-1 non-zero-based, of generics, in generics.
        typeof(User[]),
        typeof(User[][]),
        typeof(User[,]),
        typeof(User[,,]),
        typeof(User).MakeArrayType(1),
        typeof(EntityChanged<User>[]),
        typeof(EntityChanged<User[]>),
        typeof(EntityChanged<Guid[,]>),
        typeof(List<EntityChanged<User>>[]),
        typeof(GenericOuter<User>.Inner[]),
        typeof(int[]),
        // Pointer element types (never a payload, but an array of pointers is an object).
        typeof(int).MakePointerType(),
        typeof(int).MakePointerType().MakeArrayType(),
    };

    [Theory]
    [MemberData(nameof(ResolvableTypes))]
    public void Get_ReturnsAVersionLessNameThatTypeGetTypeResolvesToTheSameType(Type type)
    {
        var name = OutboxTypeName.Get(type);

        _output.WriteLine($"{name.Length} (full {type.AssemblyQualifiedName!.Length}): {name}");
        Assert.DoesNotContain("Version=", name);
        Assert.DoesNotContain("Culture=", name);
        Assert.DoesNotContain("PublicKeyToken=", name);
        Assert.Same(type, Type.GetType(name, throwOnError: true));
    }

    [Fact]
    public void Get_ForAPlainType_IsFullNameAndAssemblySimpleName()
    {
        Assert.Equal("XDev.Outbox.EFCore.Tests.Infrastructure.UserCreated, XDev.Outbox.EFCore.Tests", OutboxTypeName.Get(typeof(UserCreated)));
        Assert.Equal("XDev.Outbox.EFCore.Tests.TypeNames.OuterEvents+Nested, XDev.Outbox.EFCore.Tests", OutboxTypeName.Get(typeof(OuterEvents.Nested)));
        Assert.Equal("System.Text.Json.JsonElement, System.Text.Json", OutboxTypeName.Get(typeof(JsonElement)));
    }

    [Fact]
    public void Get_ForGenericsAndArrays_QualifiesEveryArgumentWithItsAssemblySimpleName()
    {
        Assert.Equal(
            "XDev.Outbox.EFCore.Tests.TypeNames.EntityChanged`2[[XDev.Outbox.EFCore.Tests.Infrastructure.User, XDev.Outbox.EFCore.Tests],[System.Guid, System.Private.CoreLib]], XDev.Outbox.EFCore.Tests",
            OutboxTypeName.Get(typeof(EntityChanged<User, Guid>)));
        Assert.Equal(
            "XDev.Outbox.EFCore.Tests.TypeNames.EntityChanged`1[[XDev.Outbox.EFCore.Tests.Infrastructure.User[], XDev.Outbox.EFCore.Tests]][], XDev.Outbox.EFCore.Tests",
            OutboxTypeName.Get(typeof(EntityChanged<User[]>[])));
        Assert.Equal(
            "XDev.Outbox.EFCore.Tests.TypeNames.GenericOuter`1+Inner[[XDev.Outbox.EFCore.Tests.Infrastructure.User, XDev.Outbox.EFCore.Tests]], XDev.Outbox.EFCore.Tests",
            OutboxTypeName.Get(typeof(GenericOuter<User>.Inner)));
        Assert.Equal("XDev.Outbox.EFCore.Tests.Infrastructure.User[*], XDev.Outbox.EFCore.Tests", OutboxTypeName.Get(typeof(User).MakeArrayType(1)));
        Assert.Equal("XDev.Outbox.EFCore.Tests.Infrastructure.User[,], XDev.Outbox.EFCore.Tests", OutboxTypeName.Get(typeof(User[,])));
        Assert.Equal("XDev.Outbox.EFCore.Tests.Infrastructure.User[][], XDev.Outbox.EFCore.Tests", OutboxTypeName.Get(typeof(User[][])));
    }

    [Fact]
    public void Get_ForTypicalGenericEvents_FitsTheDefault200CharacterColumnWhileTheFullNameDoesNot()
    {
        foreach (var type in new[] { typeof(EntityChanged<User>), typeof(EntityChanged<User, Guid>) })
        {
            var name = OutboxTypeName.Get(type);
            _output.WriteLine($"{type.Name}<{string.Join(", ", type.GenericTypeArguments.Select(a => a.Name))}>: full {type.AssemblyQualifiedName!.Length}, stored {name.Length}");
            Assert.True(type.AssemblyQualifiedName!.Length > 200);
            Assert.True(name.Length <= 200, $"{name} is {name.Length} characters");
        }

        // Two arguments from this assembly's long namespaces: shortened, still over 200 (the save then throws).
        var twoEntities = OutboxTypeName.Get(typeof(EntityChanged<User, Order>));
        _output.WriteLine($"EntityChanged`2<User, Order>: full {typeof(EntityChanged<User, Order>).AssemblyQualifiedName!.Length}, stored {twoEntities.Length}");
        Assert.True(twoEntities.Length > 200);
    }

    [Fact]
    public void Get_ForAnOpenGenericType_FallsBackToTheAssemblyQualifiedName()
    {
        // Never a runtime event type (no instance can have it); documented fallback only.
        Assert.Equal(typeof(EntityChanged<>).AssemblyQualifiedName, OutboxTypeName.Get(typeof(EntityChanged<>)));
    }
}
