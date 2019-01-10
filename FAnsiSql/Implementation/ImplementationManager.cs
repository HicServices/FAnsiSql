using System;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
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
        public static void Load(DirectoryInfo currentDirectory=null)
        {
            currentDirectory = currentDirectory ?? new DirectoryInfo(Environment.CurrentDirectory);

            if(!currentDirectory.Exists)
                throw new Exception("Directory '"+currentDirectory+"'did not exist");

            var catalog = new DirectoryCatalog(currentDirectory.FullName,"*FAnsi*");
            
            CompositionContainer _container = new CompositionContainer(catalog);
            _instance = new ImplementationManager();

            _container.SatisfyImportsOnce(_instance);
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
            //If no implementations are loaded, load the current directory's dlls to look for implmentations
            if (_instance == null)
                Load();
            
            var implementation = _instance.Implementations.FirstOrDefault(condition);

            if (implementation == null)
                throw new ImplementationNotFoundException(errorIfNotFound);

            return implementation;
        }

    }
}
