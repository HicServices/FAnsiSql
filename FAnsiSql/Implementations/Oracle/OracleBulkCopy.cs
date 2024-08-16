using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

internal sealed class OracleBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture) : BulkCopy(targetTable, connection, culture)
{
    private readonly DiscoveredServer _server = targetTable.Database.Server;

    public override int UploadImpl(DataTable dt)
    {
        //don't run an insert if there are 0 rows
        if (dt.Rows.Count == 0)
            return 0;

        var syntaxHelper = _server.GetQuerySyntaxHelper();
        var tt = syntaxHelper.TypeTranslater;

        //if the column name is a reserved keyword e.g. "Comment" we need to give it a new name
        var parameterNames = syntaxHelper.GetParameterNamesFor(dt.Columns.Cast<DataColumn>().ToArray(), static c => c.ColumnName);

        var affectedRows = 0;

        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());

        var dateColumns = new HashSet<DataColumn>();

        var sql = string.Format("INSERT INTO " + TargetTable.GetFullyQualifiedName() + "({0}) VALUES ({1})",
            string.Join(",", mapping.Values.Select(static c => c.GetWrappedName())),
            string.Join(",", mapping.Keys.Select(c => parameterNames[c]))
        );


        using var cmd = (OracleCommand)_server.GetCommand(sql, Connection);
        //send all the data at once
        cmd.ArrayBindCount = dt.Rows.Count;

        foreach (var (dataColumn, discoveredColumn) in mapping)
        {
            var p = _server.AddParameterWithValueToCommand(parameterNames[dataColumn], cmd, DBNull.Value);
            p.DbType = tt.GetDbTypeForSQLDBType(discoveredColumn.DataType.SQLType);

            switch (p.DbType)
            {
                case DbType.DateTime:
                    dateColumns.Add(dataColumn);
                    break;
                case DbType.Boolean:
                    p.DbType = DbType.Int32; // JS 2023-05-11 special case since we don't have a true boolean type in Oracle, but use 0/1 instead
                    break;
            }
        }

        var values = mapping.Keys.ToDictionary(static c => c, static _ => new List<object?>());

        foreach (DataRow dataRow in dt.Rows)
            //populate parameters for current row
            foreach (var col in mapping.Keys)
            {
                var val = dataRow[col];

                if (val is string stringVal && string.IsNullOrWhiteSpace(stringVal))
                    val = null;
                else if (val == DBNull.Value)
                    val = null;
                else if (dateColumns.Contains(col))
                    val = val is string s ? (DateTime?)DateTimeDecider.Parse(s) : Convert.ToDateTime(dataRow[col]);

                if (col.DataType == typeof(bool) && val is bool b)
                    values[col].Add(b ? 1 : 0);
                else
                    values[col].Add(val);
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