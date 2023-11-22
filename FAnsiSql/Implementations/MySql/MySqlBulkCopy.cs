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

    public MySqlBulkCopy(DiscoveredTable targetTable, IManagedConnection connection,CultureInfo culture) : base(targetTable, connection,culture)
    {
    }

    public override int UploadImpl(DataTable dt)
    {
        var ourTrans = Connection.Transaction==null?Connection.Connection.BeginTransaction(IsolationLevel.ReadUncommitted):null;
        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());
        var affected = 0;

        int maxPacket;
        using (var packetQ = new MySqlCommand("select @@max_allowed_packet", (MySqlConnection)Connection.Connection,(MySqlTransaction)(Connection.Transaction ?? ourTrans)))
            maxPacket = Convert.ToInt32(packetQ.ExecuteScalar());
        using var cmd = new MySqlCommand("", (MySqlConnection) Connection.Connection,
            (MySqlTransaction)(Connection.Transaction ?? ourTrans));
        if (BulkInsertBatchTimeoutInSeconds != 0)
            cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

        var commandPrefix =
            $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({string.Join(",", matchedColumns.Values.Select(c =>
                $"`{c.GetRuntimeName()}`"))}) VALUES ";

        var sb = new StringBuilder(commandPrefix,1<<22);

        var matches = matchedColumns.Keys.Select(column => (matchedColumns[column].DataType.SQLType, column.Ordinal)).ToArray();
        foreach(DataRow dr in dt.Rows)
        {
            sb.Append('(');

            var dr1 = dr;

            sb.AppendJoin(',', matches.Select(m => ConstructIndividualValue(m.SQLType, dr1[m.Ordinal])));

            sb.AppendLine("),");

            //don't let command get too long
            if (sb.Length*2<maxPacket) continue;
            cmd.CommandText = sb.ToString().TrimEnd(',', '\r', '\n');
            affected += cmd.ExecuteNonQuery();
            sb.Clear();
            sb.Append(commandPrefix);
        }

        //send final batch
        if(sb.Length > commandPrefix.Length)
        {
            cmd.CommandText = sb.ToString().TrimEnd(',', '\r', '\n');
            affected += cmd.ExecuteNonQuery();
            sb.Clear();
        }

        ourTrans?.Commit();
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
            "YEAR2" => $"'{(DateTime)DateTimeDecider.Parse(value):yy}'",
            "YEAR4" => $"'{(DateTime)DateTimeDecider.Parse(value):yyyy}'",
            _ => $"'{MySqlHelper.EscapeString(value)}'"
        };
    }
}