﻿using System.Collections.Generic;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Discovery;

/// <summary>
/// Cross database type reference to a Table valued function in a Database (actually currently only supported by Microsoft Sql Server).  For views see
/// DiscoveredTable
/// </summary>
public sealed class DiscoveredTableValuedFunction(DiscoveredDatabase database,
    string functionName,
    IQuerySyntaxHelper querySyntaxHelper,
    string? schema = null) : DiscoveredTable(database, functionName, querySyntaxHelper,
    schema, TableType.TableValuedFunction)
{
    public override bool Exists(IManagedTransaction? transaction = null)
    {
        return Database.DiscoverTableValuedFunctions(transaction).Any(f => f.GetRuntimeName().Equals(GetRuntimeName()));
    }

    public override string GetRuntimeName() => QuerySyntaxHelper.GetRuntimeName(TableName);

    public override string GetFullyQualifiedName()
    {
        //This is pretty inefficient that we have to go discover these in order to tell you how to invoke the table!
        var parameters = string.Join(",", DiscoverParameters().Select(static p => p.ParameterName));

        //Note that we do not give the parameters values, the client must decide appropriate values and put them in correspondingly named variables
        return $"{Database.GetRuntimeName()}..{GetRuntimeName()}({parameters})";
    }

    public override string ToString() => TableName;

    public override void Drop()
    {
        using var connection = Database.Server.GetManagedConnection();
        Helper.DropFunction(connection.Connection, this);
    }

    public IEnumerable<DiscoveredParameter> DiscoverParameters(ManagedTransaction? transaction = null)
    {
        using var connection = Database.Server.GetManagedConnection(transaction);
        foreach (var param in Helper.DiscoverTableValuedFunctionParameters(connection.Connection, this,
                     connection.Transaction))
            yield return param;
    }
}