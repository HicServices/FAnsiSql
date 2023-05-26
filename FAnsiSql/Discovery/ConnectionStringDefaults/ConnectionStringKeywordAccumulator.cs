using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Implementation;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Discovery.ConnectionStringDefaults;

/// <summary>
/// <para>Gathers keywords for use in building connection strings for a given <see cref="DatabaseType"/>.  Once created you can add keywords and then apply the template 
/// to new novel connection strings (see <see cref="EnforceOptions"/>).</para>
/// 
/// <para>Also handles connection string keyword aliases (where two words mean the same thing)</para>
/// </summary>
public class ConnectionStringKeywordAccumulator
{
    /// <summary>
    /// <see cref="DatabaseType"/> describing what implmentation of DbConnectionStringBuilder is being manipulated
    /// </summary>
    public DatabaseType DatabaseType { get; private set; }

    private readonly Dictionary<string, Tuple<string, ConnectionStringKeywordPriority>> _keywords = new(StringComparer.CurrentCultureIgnoreCase);
    private readonly DbConnectionStringBuilder _builder;

    /// <summary>
    /// Initialises a new blank instance that does nothing.  Call <see cref="AddOrUpdateKeyword"/> to adjust the template connection string options.
    /// </summary>
    /// <param name="databaseType"></param>
    public ConnectionStringKeywordAccumulator(DatabaseType databaseType)
    {
        DatabaseType = databaseType;
        _builder = ImplementationManager.GetImplementation(databaseType).GetBuilder();
    }

    /// <summary>
    /// Adds a new connection string option (which must be compatible with <see cref="DatabaseType"/>) 
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="value"></param>
    /// <param name="priority"></param>
    public void AddOrUpdateKeyword(string keyword, string value, ConnectionStringKeywordPriority priority)
    {
        var collision = GetCollisionWithKeyword(keyword,value);

        if (collision != null)
        {
            //if there is already a semantically equivalent keyword.... 

            //if it is of lower or equal priority
            if (_keywords[collision].Item2 <= priority)
                _keywords[collision] = Tuple.Create(value, priority); //update it 
                
            //either way don't record it as a new keyword
            return;
        }

        //if we have not got that keyword yet
        if(!_keywords.TryAdd(keyword, Tuple.Create(value, priority)) && _keywords[keyword].Item2 <= priority)
        {
            //or the keyword that was previously specified had a lower priority
            _keywords[keyword] = Tuple.Create(value, priority); //update it with the new value
        }
    }

    /// <summary>
    /// Returns the best alias for <paramref name="keyword"/> or null if there are no known aliases.  This is because some builders allow multiple keys for changing the same underlying
    /// property.
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    private string GetCollisionWithKeyword(string keyword, string value)
    {
        ArgumentNullException.ThrowIfNull(keyword);
        ArgumentNullException.ThrowIfNull(value);

        //let's evaluate this alleged keyword!
        _builder.Clear();

        try
        {
            //Make sure it is supported by the connection string builder
            _builder.Add(keyword, value);
        }
        catch (OracleException ex)
        {
            //don't output the value since that could be a password
            throw new ArgumentException(string.Format(FAnsiStrings.ConnectionStringKeyword_ValueNotSupported, keyword), ex);
        }
        catch (NotSupportedException ex)
        {
            //don't output the value since that could be a password
            throw new ArgumentException(string.Format(FAnsiStrings.ConnectionStringKeyword_ValueNotSupported, keyword),ex);
        }            

        //now iterate all the keys we had before and add those too, if the key count doesn't change for any of them we know it's a duplicate semantically
        if (_builder.Keys == null) return null;
        foreach (var current in _keywords)
        {
            var keysBefore = _builder.Keys.Count;

            _builder.Add(current.Key, current.Value.Item1);

            //key count in builder didn't change despite there being new values added
            if (_builder.Keys.Count == keysBefore)
                return current.Key;
        }

        //no collisions
        return null;
    }

    /// <summary>
    /// Adds the currently configured keywords to the connection string builder.
    /// </summary>
    public void EnforceOptions(DbConnectionStringBuilder connectionStringBuilder)
    {
        foreach (var keyword in _keywords)
            connectionStringBuilder[keyword.Key] = keyword.Value.Item1;
    }
}