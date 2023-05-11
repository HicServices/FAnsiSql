using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

internal class OracleBulkCopy : BulkCopy
{
    private readonly DiscoveredServer _server;
    private const char ParameterSymbol = ':';

    public OracleBulkCopy(DiscoveredTable targetTable, IManagedConnection connection,CultureInfo culture): base(targetTable, connection,culture)
    {
        _server = targetTable.Database.Server;
    }
        
    public override int UploadImpl(DataTable dt)
    {
        //don't run an insert if there are 0 rows
        if (dt.Rows.Count == 0)
            return 0;
                        
        var syntaxHelper = _server.GetQuerySyntaxHelper();
        var tt = syntaxHelper.TypeTranslater;

        //if the column name is a reserved keyword e.g. "Comment" we need to give it a new name
        var parameterNames = syntaxHelper.GetParameterNamesFor(dt.Columns.Cast<DataColumn>().ToArray(),c=>c.ColumnName);

        var affectedRows = 0;
            
        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());

        var dateColumns = new HashSet<DataColumn>();

        var sql = string.Format("INSERT INTO " + TargetTable.GetFullyQualifiedName() + "({0}) VALUES ({1})",
            string.Join(",", mapping.Values.Select(c=> $"\"{c.GetWrappedName()}\"")),
            string.Join(",", mapping.Keys.Select(c => parameterNames[c]))
        );


        using var cmd = (OracleCommand) _server.GetCommand(sql, Connection);
        //send all the data at once
        cmd.ArrayBindCount = dt.Rows.Count;

        foreach (var kvp in mapping)
        {
            var p = _server.AddParameterWithValueToCommand(parameterNames[kvp.Key], cmd, DBNull.Value);
            p.DbType = tt.GetDbTypeForSQLDBType(kvp.Value.DataType.SQLType);

            if (p.DbType == DbType.DateTime)
                dateColumns.Add(kvp.Key);
        }
                
        var values = mapping.Keys.ToDictionary(c => c, _ => new List<object>());

        foreach (DataRow dataRow in dt.Rows)
        {
            //populate parameters for current row
            foreach (var col in mapping.Keys)
            {
                var val = dataRow[col];

                if (val is string stringVal && string.IsNullOrWhiteSpace(stringVal))
                    val = null;
                else
                if (val == null || val == DBNull.Value)
                    val = null;
                else if (dateColumns.Contains(col))
                    val = val is string s ? (DateTime)DateTimeDecider.Parse(s) : Convert.ToDateTime(dataRow[col]);

                values[col].Add(val);
            }
        }

        foreach (var col in mapping.Keys)
        {
            var param = cmd.Parameters[parameterNames[col]];
            param.Value = values[col].ToArray();
        }

        //send query
        affectedRows += cmd.ExecuteNonQuery();
        return affectedRows;
    }
}