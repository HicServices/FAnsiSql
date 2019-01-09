using System;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using FAnsi.Exceptions;

namespace FAnsi.Implementation
{
    /// <summary>
    /// Handles detecting and loading implementations
    /// </summary>
    public class ImplementationManager
    {
        private static ImplementationManager _instance;

        [ImportMany]
        public IImplementation[] Implementations { get; set; }

        /// <summary>
        /// Loads all implementations found in currently loaded assemblies (in the current domain)
        /// </summary>
        public static void Load()
        {
            Load(AppDomain.CurrentDomain.GetAssemblies());
        }

        /// <summary>
        /// Loads all implementations found in the provided assemblies
        /// </summary>
        /// <param name="assemblies"></param>
        public static void Load(params Assembly[] assemblies)
        {
            AggregateCatalog catalog = new AggregateCatalog();

            foreach (Assembly assembly in assemblies)
                catalog.Catalogs.Add(new AssemblyCatalog(assembly));

            CompositionContainer _container = new CompositionContainer(catalog);
            _instance = new ImplementationManager();

            _container.SatisfyImportsOnce(_instance);
        }

        public static IImplementation GetImplementation(DatabaseType databaseType)
        {
            return GetImplementation(i => i.IsFor(databaseType),"No implementation found for DatabaseType " + databaseType);
        }

        public static IImplementation GetImplementation(DbConnectionStringBuilder connectionStringBuilder)
        {
            return GetImplementation(i => i.IsFor(connectionStringBuilder),"No implementation found for Type " + connectionStringBuilder.GetType());
        }

        public static IImplementation GetImplementation(DbConnection connection)
        {
            return GetImplementation(i => i.IsFor(connection), "No implementation found for Type " + connection.GetType());
        }
        private static IImplementation GetImplementation(Func<IImplementation,bool> condition, string errorIfNotFound)
        {
            if (_instance == null)
                throw new Exception("Instance has not been set yet, try calling ImplementationManager.Load");

            var implementation = _instance.Implementations.FirstOrDefault(condition);

            if (implementation == null)
                throw new ImplementationNotFoundException(errorIfNotFound);

            return implementation;
        }

    }
}
