using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Connections;
using FAnsi.Discovery;
using MySqlConnector;

namespace FAnsi.Implementations.MySql
{
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
            int affected = 0;

            using (var cmd = new MySqlCommand("", (MySqlConnection) Connection.Connection,
                (MySqlTransaction) Connection.Transaction))
            {
                if (BulkInsertBatchTimeoutInSeconds != 0)
                    cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

                string commandPrefix = string.Format("INSERT INTO {0}({1}) VALUES ", TargetTable.GetFullyQualifiedName(),string.Join(",", matchedColumns.Values.Select(c => "`" + c.GetRuntimeName() + "`")));

                StringBuilder sb = new StringBuilder();
                
                
                int row = 0;

                foreach(DataRow dr in dt.Rows)
                {
                    sb.Append('(');

                    DataRow dr1 = dr;
                
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
            }

            return affected;
            
        }

        private string ConstructIndividualValue(string dataType, object value)
        {
            dataType = dataType.ToUpper();
            dataType = Regex.Replace(dataType,"\\(.*\\)", "").Trim();

            if(value is DateTime valueDateTime)
                switch(dataType)
                {
                    case "DATE":
                    return String.Format("'{0:yyyy-MM-dd}'", valueDateTime);
                    case "TIMESTAMP":
                    case "DATETIME":
                        return String.Format("'{0:yyyy-MM-dd HH:mm:ss}'", valueDateTime);
                    case "TIME":
                        return String.Format("'{0:HH:mm:ss}'", valueDateTime);
                    default :
                        break;
                }

            if(value == null || value == DBNull.Value)
                return "NULL";
            
            return ConstructIndividualValue(dataType,  value.ToString());
        }

        private string ConstructIndividualValue(string dataType, string value)
        {
            switch (dataType)
            {
                case "BIT":
                    return value;

                //Numbers
                case "INT":
                case "TINYINT":
                case "SMALLINT":
                case "MEDIUMINT":
                case "BIGINT":
                case "FLOAT":
                case "DOUBLE":
                case "DECIMAL":
                    return string.Format("{0}", value);
                
                //Text
                case "CHAR":
                case "VARCHAR":
                case "BLOB":
                case "TEXT":
                case "TINYBLOB":
                case "TINYTEXT":
                case "MEDIUMBLOB":
                case "MEDIUMTEXT":
                case "LONGBLOB":
                case "LONGTEXT":
                case "ENUM":
                    return string.Format("'{0}'", MySqlHelper.EscapeString(value));
                
                //Dates/times
                case "DATE":
                    return String.Format("'{0:yyyy-MM-dd}'", (DateTime)DateTimeDecider.Parse(value));
                case "TIMESTAMP":
                case "DATETIME":
                    return String.Format("'{0:yyyy-MM-dd HH:mm:ss}'", (DateTime)DateTimeDecider.Parse(value));
                case "TIME":
                    return String.Format("'{0:HH:mm:ss}'", (DateTime)DateTimeDecider.Parse(value));
                case "YEAR2":
                    return String.Format("'{0:yy}'", value);
                case "YEAR4":
                    return String.Format("'{0:yyyy}'", value);

                //Unknown
                default:
                    // we don't understand the format. to safegaurd the code, just enclose with ''
                    return string.Format("'{0}'", MySqlHelper.EscapeString(value));
            }
        }
    }
}