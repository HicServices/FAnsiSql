using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Connections;
using FAnsi.Discovery;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

/// <summary>
/// Inserts rows into MySql table using extended INSERT commands.  'LOAD DATA IN FILE' is not used because it doesn't respect table constraints, can be disabled
/// on the server and generally can go wrong in a large number of ways.
/// </summary>
public class MySqlBulkCopy : BulkCopy
{

    public static int BulkInsertBatchTimeoutInSeconds = 0;

    /// <summary>
    /// The number of rows to send in each INSERT statement to the server
    /// 
    /// <para>Inserting into MySql without using 'LOAD DATA IN FILE' means using extended INSERT.  This takes the form INSERT INTO Tbl(a,b,c) Values (1,2,3),(4,5,6),(7,8,9) etc.
    /// If you send too many rows at once then MySql complains due to network packet size (See https://dev.mysql.com/doc/refman/5.5/en/packet-too-large.html).
    /// </para>
    /// 
    /// </summary>
    public static int BulkInsertRowsPerNetworkPacket = 500;

    public MySqlBulkCopy(DiscoveredTable targetTable, IManagedConnection connection,CultureInfo culture) : base(targetTable, connection,culture)
    {
    }

    public override int UploadImpl(DataTable dt)
    {
        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());
        var affected = 0;

        using var cmd = new MySqlCommand("", (MySqlConnection) Connection.Connection,
            (MySqlTransaction) Connection.Transaction);
        if (BulkInsertBatchTimeoutInSeconds != 0)
            cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

        var commandPrefix =
            $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({string.Join(",", matchedColumns.Values.Select(c =>
                $"`{c.GetRuntimeName()}`"))}) VALUES ";

        var sb = new StringBuilder();
                
                
        var row = 0;

        foreach(DataRow dr in dt.Rows)
        {
            sb.Append('(');

            var dr1 = dr;
                
            sb.Append(string.Join(",",matchedColumns.Keys.Select(k => ConstructIndividualValue(matchedColumns[k].DataType.SQLType,dr1[k]))));

            sb.AppendLine("),");
            row++;

            //don't let command get too long
            if (row % BulkInsertRowsPerNetworkPacket == 0)
            {
                cmd.CommandText = commandPrefix + sb.ToString().TrimEnd(',', '\r', '\n');
                affected += cmd.ExecuteNonQuery();
                sb.Clear();
            }
        }

        //send final batch
        if(sb.Length > 0)
        {
            cmd.CommandText = commandPrefix + sb.ToString().TrimEnd(',', '\r', '\n');
            affected += cmd.ExecuteNonQuery();
            sb.Clear();
        }

        return affected;
            
    }

    private string ConstructIndividualValue(string dataType, object value)
    {
        dataType = dataType.ToUpper();
        dataType = Regex.Replace(dataType,"\\(.*\\)", "").Trim();

        if(value is DateTime valueDateTime)
            switch (dataType)
            {
                case "DATE":
                    return $"'{valueDateTime:yyyy-MM-dd}'";
                case "TIMESTAMP" or "DATETIME":
                    return $"'{valueDateTime:yyyy-MM-dd HH:mm:ss}'";
                case "TIME":
                    return $"'{valueDateTime:HH:mm:ss}'";
            }

        if(value == null || value == DBNull.Value)
            return "NULL";
            
        return ConstructIndividualValue(dataType,  value.ToString());
    }

    private string ConstructIndividualValue(string dataType, string value)
    {
        return dataType switch
        {
            "BIT" => value,
            //Numbers
            "INT" => $"{value}",
            "TINYINT" => $"{value}",
            "SMALLINT" => $"{value}",
            "MEDIUMINT" => $"{value}",
            "BIGINT" => $"{value}",
            "FLOAT" => $"{value}",
            "DOUBLE" => $"{value}",
            "DECIMAL" => $"{value}",
            //Text
            "CHAR" => $"'{MySqlHelper.EscapeString(value)}'",
            "VARCHAR" => $"'{MySqlHelper.EscapeString(value)}'",
            "BLOB" => $"'{MySqlHelper.EscapeString(value)}'",
            "TEXT" => $"'{MySqlHelper.EscapeString(value)}'",
            "TINYBLOB" => $"'{MySqlHelper.EscapeString(value)}'",
            "TINYTEXT" => $"'{MySqlHelper.EscapeString(value)}'",
            "MEDIUMBLOB" => $"'{MySqlHelper.EscapeString(value)}'",
            "MEDIUMTEXT" => $"'{MySqlHelper.EscapeString(value)}'",
            "LONGBLOB" => $"'{MySqlHelper.EscapeString(value)}'",
            "LONGTEXT" => $"'{MySqlHelper.EscapeString(value)}'",
            "ENUM" => $"'{MySqlHelper.EscapeString(value)}'",
            //Dates/times
            "DATE" => $"'{(DateTime)DateTimeDecider.Parse(value):yyyy-MM-dd}'",
            "TIMESTAMP" => $"'{(DateTime)DateTimeDecider.Parse(value):yyyy-MM-dd HH:mm:ss}'",
            "DATETIME" => $"'{(DateTime)DateTimeDecider.Parse(value):yyyy-MM-dd HH:mm:ss}'",
            "TIME" => $"'{(DateTime)DateTimeDecider.Parse(value):HH:mm:ss}'",
            "YEAR2" => $"'{value:yy}'",
            "YEAR4" => $"'{value:yyyy}'",
            _ => $"'{MySqlHelper.EscapeString(value)}'"
        };
    }
}