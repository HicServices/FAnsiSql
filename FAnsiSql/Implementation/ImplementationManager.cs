using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using FAnsi.Exceptions;

namespace FAnsi.Implementation;

/// <summary>
/// Handles detecting and loading implementations
/// </summary>
public sealed class ImplementationManager
{
    private static readonly ImplementationManager Instance=new();

    /// <summary>
    /// Collection of all the currently loaded API <see cref="IImplementation"/>.  Normally you only want 1 implementation per DBMS.
    /// </summary>
    private readonly List<IImplementation> _implementations;

    private ImplementationManager()
    {
        _implementations = [];
    }

    /// <summary>
    /// loads all implementations in the assembly hosting the <typeparamref name="T"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public static void Load<T>() where T:IImplementation,new()
    {
        var loading = new T();
        if (!Instance._implementations.Contains(loading))
            Instance._implementations.Add(loading);
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
    private static IImplementation GetImplementation(Func<IImplementation,bool> condition, string errorIfNotFound) => Instance?._implementations.FirstOrDefault(condition)??throw new ImplementationNotFoundException(errorIfNotFound);

    /// <summary>
    /// Returns all currently loaded implementations or null if no implementations have been loaded
    /// </summary>
    /// <returns></returns>
    public static ReadOnlyCollection<IImplementation> GetImplementations() => Instance._implementations.AsReadOnly();

    /// <summary>
    /// Clears all currently loaded <see cref="IImplementation"/>
    /// </summary>
    public static void Clear()
    {
        Instance._implementations.Clear();
    }

    [Obsolete("MEF is dead")]
    public static void Load(params Assembly[] _)
    {
    }
}