using System;
using System.IO;
using System.Linq;
using System.Reflection;

namespace XDev.Outbox
{
    /// <summary>
    /// Resolves the type name stored in <see cref="OutboxMessage{TKey}.Type"/>: a full assembly-qualified name (older
    /// rows) or one without Version, Culture and PublicKeyToken (the EF store's rows).
    /// </summary>
    /// <remarks>
    /// Every assembly is first loaded by name exactly as <see cref="Type.GetType(string)"/> does. Only when that fails is
    /// an already-loaded assembly with the same name used: the runtime remembers a failed load by simple name, so an
    /// assembly loaded later by path (a plugin loaded with <c>Assembly.LoadFrom</c> or
    /// <c>AssemblyLoadContext.LoadFromAssemblyPath</c>) would otherwise never resolve for that name until the process
    /// restarts.
    /// <para>
    /// Cost: a name that resolves only through this fallback costs a first-chance <see cref="FileNotFoundException"/>
    /// plus a scan of the loaded assemblies on every resolution. Nothing is cached by design: a failed resolution must
    /// not be remembered, so a plugin loaded later still resolves.
    /// </para>
    /// </remarks>
    internal static class OutboxEventTypeResolver
    {
        /// <summary>The type, or null when it or one of its assemblies cannot be found.</summary>
        /// <exception cref="FileLoadException">An assembly was found but could not be loaded (as <see cref="Type.GetType(string)"/>).</exception>
        /// <exception cref="BadImageFormatException">An assembly file is not valid (as <see cref="Type.GetType(string)"/>).</exception>
        public static Type Resolve(string typeName)
            => Type.GetType(typeName, ResolveAssembly, typeResolver: null, throwOnError: false);

        private static Assembly ResolveAssembly(AssemblyName name)
        {
            try
            {
                return Assembly.Load(name);
            }
            catch (FileNotFoundException)
            {
                return FindLoadedAssembly(name);
            }
            catch (Exception ex) when (ex is FileLoadException || ex is BadImageFormatException)
            {
                // Keep the load error when no loaded assembly can stand in for it.
                var loaded = FindLoadedAssembly(name);
                if (loaded == null) throw;
                return loaded;
            }
        }

        /// <summary>
        /// A loaded assembly with the same simple name. When the name has a Version, the loaded assembly's version must be
        /// the same or higher (as the runtime binder accepts); when it has a non-empty PublicKeyToken, the token must be
        /// equal. A null or empty requested token (<c>PublicKeyToken=null</c> or none) skips the token check.
        /// Several candidates: the default load context first, then the highest version, then the first loaded.
        /// </summary>
        private static Assembly FindLoadedAssembly(AssemblyName name)
        {
            var requestedToken = name.GetPublicKeyToken();
            Assembly best = null;
            AssemblyName bestName = null;
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                var candidate = assembly.GetName();
                if (!string.Equals(candidate.Name, name.Name, StringComparison.OrdinalIgnoreCase)) continue;
                if (name.Version != null && (candidate.Version == null || candidate.Version < name.Version)) continue;
                if (requestedToken != null && requestedToken.Length > 0
                    && !requestedToken.SequenceEqual(candidate.GetPublicKeyToken() ?? Array.Empty<byte>())) continue;

                if (best == null || IsPreferred(assembly, candidate, best, bestName))
                {
                    best = assembly;
                    bestName = candidate;
                }
            }
            return best;
        }

        private static bool IsPreferred(Assembly assembly, AssemblyName name, Assembly best, AssemblyName bestName)
        {
#if NETCOREAPP
            var isDefault = System.Runtime.Loader.AssemblyLoadContext.GetLoadContext(assembly) == System.Runtime.Loader.AssemblyLoadContext.Default;
            var bestIsDefault = System.Runtime.Loader.AssemblyLoadContext.GetLoadContext(best) == System.Runtime.Loader.AssemblyLoadContext.Default;
            if (isDefault != bestIsDefault) return isDefault;
#endif
            return (name.Version ?? new Version()) > (bestName.Version ?? new Version());
        }
    }
}
