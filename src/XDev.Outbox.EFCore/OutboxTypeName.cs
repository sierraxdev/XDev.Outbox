using System;
using System.Reflection;
using System.Text;

namespace XDev.Outbox.EFCore
{
    /// <summary>
    /// Builds the name stored in <c>OutboxMessage.Type</c>: the assembly-qualified name without Version, Culture and
    /// PublicKeyToken, at every level (generic arguments included), e.g.
    /// <c>Ns.EntityChanged`1[[Ns2.Order, Domain]], Contracts</c>. <see cref="Type.GetType(string)"/> resolves it by the
    /// assemblies' simple names, so it stays loadable and is much shorter than <see cref="Type.AssemblyQualifiedName"/>.
    /// </summary>
    internal static class OutboxTypeName
    {
        public static string Get(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            // Open generic types and generic parameters have no loadable name; never a runtime event type.
            if (type.ContainsGenericParameters) return type.AssemblyQualifiedName;

            var builder = new StringBuilder();
            AppendQualified(builder, type, genericArgument: false);
            return builder.ToString();
        }

        private static void AppendQualified(StringBuilder builder, Type type, bool genericArgument)
        {
            AppendName(builder, type);
            var assemblyName = AssemblyName(type);
            // AssemblyName does not escape ']'; inside a bracketed generic argument an unescaped ']' would end the
            // argument early, so it is escaped there as in AssemblyQualifiedName. Not at the top level: there the escaped
            // form without a Version does not load (net8).
            if (genericArgument) assemblyName = assemblyName.Replace("]", @"\]");
            builder.Append(", ").Append(assemblyName);
        }

        /// <summary>The type name without its assembly (generic arguments are assembly-qualified, as in FullName).</summary>
        private static void AppendName(StringBuilder builder, Type type)
        {
            if (type.IsArray)
            {
                AppendName(builder, type.GetElementType());
                var rank = type.GetArrayRank();
                if (type.IsSZArray) builder.Append("[]");
                else if (rank == 1) builder.Append("[*]");
                else builder.Append('[').Append(',', rank - 1).Append(']');
                return;
            }

            if (type.IsPointer)
            {
                AppendName(builder, type.GetElementType());
                builder.Append('*');
                return;
            }

            if (type.IsByRef)
            {
                AppendName(builder, type.GetElementType());
                builder.Append('&');
                return;
            }

            if (type.IsConstructedGenericType)
            {
                // The definition's FullName covers namespace, declaring types ("+") and arity ("`n").
                builder.Append(type.GetGenericTypeDefinition().FullName).Append('[');
                var arguments = type.GenericTypeArguments;
                for (var i = 0; i < arguments.Length; i++)
                {
                    if (i > 0) builder.Append(',');
                    builder.Append('[');
                    AppendQualified(builder, arguments[i], genericArgument: true);
                    builder.Append(']');
                }
                builder.Append(']');
                return;
            }

            builder.Append(type.FullName);
        }

        /// <summary>Simple name of the assembly defining the innermost element type, escaped as in a display name.</summary>
        private static string AssemblyName(Type type)
        {
            while (type.HasElementType) type = type.GetElementType();
            return new AssemblyName { Name = type.Assembly.GetName().Name }.FullName;
        }
    }
}
