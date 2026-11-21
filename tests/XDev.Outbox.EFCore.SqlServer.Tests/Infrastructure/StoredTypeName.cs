namespace XDev.Outbox.EFCore.SqlServer.Tests.Infrastructure;

/// <summary>
/// The name the library stores in OutboxMessage.Type for a non-generic, non-array event type: its full name and its
/// assembly's simple name (no Version, Culture or PublicKeyToken). Generic names are spelled out in the tests.
/// </summary>
public static class StoredTypeName
{
    public static string Of(Type type)
    {
        if (type.IsGenericType || type.HasElementType) throw new ArgumentException("Only plain types are supported.", nameof(type));
        return $"{type.FullName}, {type.Assembly.GetName().Name}";
    }

    public static string Of<T>() => Of(typeof(T));
}
