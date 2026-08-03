using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace XDev.Outbox
{
    public static class ObjectExtensions
    {
        public static ExpandoObject ToExpando(this object source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            var options = new JsonSerializerOptions()
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };
            options.Converters.Add(new JsonStringEnumConverter());

            var json = JsonSerializer.Serialize(source, options);
            var expando = JsonSerializer.Deserialize<ExpandoObject>(json);

            return expando;
        }

        public static T ToObject<T>(this ExpandoObject source) where T : new()
        {
            return (T)source.ToObject(typeof(T));
        }

        public static object ToObject(this ExpandoObject expando, Type type)
        {
            if(!type.IsClass) throw new ArgumentException("Type must be a class.", nameof(type));

            var target = Activator.CreateInstance(type);
            var dict = (IDictionary<string, object>)expando;
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var prop in props)
            {
                if (dict.TryGetValue(OutboxConfiguration.SerializerOptions.PropertyNamingPolicy.ConvertName(prop.Name), out var value) && prop.CanWrite)
                {
                    if (value is ExpandoObject nestedExpando && !prop.PropertyType.IsPrimitive && prop.PropertyType != typeof(string))
                    {
                        // Recurse for nested objects
                        var nestedMethod = typeof(ObjectExtensions)
                            .GetMethod(nameof(ToObject))
                            .MakeGenericMethod(prop.PropertyType);
                        prop.SetValue(target, nestedMethod.Invoke(null, new object[] { nestedExpando, prop.PropertyType }));
                    }
                    else
                    {
                        if(value.GetType() == typeof(JsonElement))
                            prop.SetValue(target, ((JsonElement)value).ToObject(prop.PropertyType));
                        else
                            prop.SetValue(target,  Convert.ChangeType(value, prop.PropertyType));
                    }
                }
            }

            return target;
        }

        public static object ToObject(this JsonElement element, Type targetType)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Null:
                case JsonValueKind.Undefined:
                    return null;

                case JsonValueKind.True:
                case JsonValueKind.False:
                    return Convert.ChangeType(element.GetBoolean(), Nullable.GetUnderlyingType(targetType) ?? targetType);

                case JsonValueKind.Number:
                    return ConvertNumber(element, Nullable.GetUnderlyingType(targetType) ?? targetType);

                case JsonValueKind.String:
                case JsonValueKind.Object:
                case JsonValueKind.Array:
                    // Covers DateTime/Guid/enum strings and nested objects/arrays.
                    return JsonSerializer.Deserialize(element.GetRawText(), targetType, options: OutboxConfiguration.SerializerOptions);

                default:
                    throw new ArgumentOutOfRangeException(nameof(element), element.ValueKind, "Unsupported JsonValueKind.");
            }
        }

        public static T ToObject<T>(this JsonElement element) =>
            (T)element.ToObject(typeof(T));

        private static object ConvertNumber(JsonElement element, Type targetType)
        {
            if (targetType == typeof(int)) return element.GetInt32();
            if (targetType == typeof(long)) return element.GetInt64();
            if (targetType == typeof(short)) return element.GetInt16();
            if (targetType == typeof(byte)) return element.GetByte();
            if (targetType == typeof(double)) return element.GetDouble();
            if (targetType == typeof(float)) return element.GetSingle();
            if (targetType == typeof(decimal)) return element.GetDecimal();
            if (targetType == typeof(uint)) return element.GetUInt32();
            if (targetType == typeof(ulong)) return element.GetUInt64();

            // Fallback for object/dynamic targets.
            return element.TryGetInt64(out var l) ? l : element.GetDouble();
        }
    }
}
