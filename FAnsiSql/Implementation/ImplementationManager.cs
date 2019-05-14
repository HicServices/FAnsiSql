using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
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
        private List<IImplementation> _implementations;

        private ImplementationManager()
        {

        }

        /// <summary>
        /// loads all implementations in the assembly hosting the <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public static void Load<T>() where T:IImplementation
        {
            Load(typeof(T).Assembly);
        }

        /// <summary>
        /// Loads all implementations found in currently loaded assemblies (in the current domain)
        /// </summary>
        public static void Load(DirectoryInfo currentDirectory=null)
        {
            currentDirectory = currentDirectory ?? new DirectoryInfo(Environment.CurrentDirectory);

            if(!currentDirectory.Exists)
                throw new Exception("Directory '"+currentDirectory+"'did not exist");

            var catalog = new DirectoryCatalog(currentDirectory.FullName,"*FAnsi*");
                        
            Load(catalog);
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

            Load(catalog);
        }

        private static  void Load(ComposablePartCatalog catalog)
        {
            if (_instance == null)
                _instance = new ImplementationManager();

            //just because we load new implementations doesn't mean we should throw away the old ones
            var old = _instance._implementations?.ToArray();

            using (CompositionContainer container = new CompositionContainer(catalog))
            {
                //bring in the new ones
                container.SatisfyImportsOnce(_instance);

                //but also bring in any old ones that we don't have in the new load
                if(old != null)
                    foreach(IImplementation o in old)
                    {
                        if(_instance._implementations.All(i=>i.GetType() != o.GetType()))
                            _instance._implementations.Add(o);
                    }
            }
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
            
            var implementation = _instance._implementations.FirstOrDefault(condition);

            if (implementation == null)
                throw new ImplementationNotFoundException(errorIfNotFound);

            return implementation;
        }

        /// <summary>
        /// Returns all currently loaded implementations or null if no implementations have been loaded
        /// </summary>
        /// <returns></returns>
        public static ReadOnlyCollection<IImplementation> GetImplementations()
        {
            //If no implementations are loaded, load the current directory's dlls to look for implmentations
            if (_instance == null)
                return null;

            if(_instance._implementations == null)
                return null;

            return new ReadOnlyCollection<IImplementation>(_instance._implementations);
        }

        public static void Clear()
        {
            _instance = null;
        }
    }
}
