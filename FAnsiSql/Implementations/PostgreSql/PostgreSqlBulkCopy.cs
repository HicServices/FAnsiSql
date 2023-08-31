using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public class PostgreSqlBulkCopy : BulkCopy
{
    public PostgreSqlBulkCopy(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture) :base(discoveredTable,connection,culture)
    {
    }



    public override int UploadImpl(DataTable dt)
    {
        var con = (NpgsqlConnection) Connection.Connection;

        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());

        //see https://www.npgsql.org/doc/copy.html
        var sb = new StringBuilder();

        sb.Append("COPY ");
        sb.Append(TargetTable.GetFullyQualifiedName());
        sb.Append(" (");
        sb.AppendJoin(",", matchedColumns.Values.Select(v => v.GetWrappedName()));
        sb.Append(')');
        sb.Append(" FROM STDIN (FORMAT BINARY)");

        var tt = PostgreSqlTypeTranslater.Instance;

        var dataColumns = matchedColumns.Keys.ToArray();
        var types = matchedColumns.Keys.Select(v => tt.GetNpgsqlDbTypeForCSharpType(v.DataType)).ToArray();
            
        using (var import = con.BeginBinaryImport(sb.ToString()))
        {
            foreach (DataRow r in dt.Rows)
            {
                import.StartRow();

                for (var index = 0; index < dataColumns.Length; index++)
                {
                    var dc = dataColumns[index];
                    if (r[dc] == DBNull.Value)
                        import.WriteNull();
                    else
                        import.Write(r[dc],types[index]);
                }
            }

            import.Complete();
        }

        return dt.Rows.Count;
    }
}