using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using FAnsi.Exceptions;

namespace FAnsi.Implementation;

/// <summary>
/// Handles detecting and loading implementations
/// </summary>
public class ImplementationManager
{
    private static ImplementationManager _instance;

    /// <summary>
    /// Collection of all the currently loaded API <see cref="IImplementation"/>.  Normally you only want 1 implementation per DBMS.
    /// <para>Populated by MEF during calls to <see cref="Load{T}"/>.</para>
    /// </summary>
    [ImportMany]
    public List<IImplementation> Implementations;

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
            throw new Exception(string.Format(FAnsiStrings.ImplementationManager_Load_Directory___0__did_not_exist, currentDirectory));

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
        var old = _instance.Implementations?.ToArray();

        using (CompositionContainer container = new CompositionContainer(catalog))
        {
            //bring in the new ones
            container.SatisfyImportsOnce(_instance);

            //but also bring in any old ones that we don't have in the new load
            if(old != null)
                foreach(IImplementation o in old)
                {
                    if(_instance.Implementations.All(i=>i.GetType() != o.GetType()))
                        _instance.Implementations.Add(o);
                }
        }
    }

    public static IImplementation GetImplementation(DatabaseType databaseType)
    {
        return GetImplementation(i => i.IsFor(databaseType),
            string.Format(
                FAnsiStrings.ImplementationManager_GetImplementation_No_implementation_found_for_DatabaseType__0_,
                databaseType));
    }

    public static IImplementation GetImplementation(DbConnectionStringBuilder connectionStringBuilder)
    {
        return GetImplementation(i => i.IsFor(connectionStringBuilder),
            string.Format(
                FAnsiStrings
                    .ImplementationManager_GetImplementation_No_implementation_found_for_ADO_Net_object_of_Type__0_,
                connectionStringBuilder.GetType()));
    }

    public static IImplementation GetImplementation(DbConnection connection)
    {
        return GetImplementation(i => i.IsFor(connection), 
            string.Format(
                FAnsiStrings
                    .ImplementationManager_GetImplementation_No_implementation_found_for_ADO_Net_object_of_Type__0_,
                connection.GetType()));
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

    /// <summary>
    /// Returns all currently loaded implementations or null if no implementations have been loaded
    /// </summary>
    /// <returns></returns>
    public static ReadOnlyCollection<IImplementation> GetImplementations()
    {
        //If no implementations are loaded, load the current directory's dlls to look for implmentations
        if (_instance == null)
            return null;

        if(_instance.Implementations == null)
            return null;

        return new ReadOnlyCollection<IImplementation>(_instance.Implementations);
    }

    /// <summary>
    /// Clears all currently loaded <see cref="IImplementation"/>
    /// </summary>
    public static void Clear()
    {
        _instance = null;
    }
}