using System;
using System.Data;
using System.Globalization;
using FAnsi.Connections;
using FAnsi.Discovery;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlBulkCopy : BulkCopy
    {
        public PostgreSqlBulkCopy(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture) :base(discoveredTable,connection,culture)
        {
        }

        public override int UploadImpl(DataTable dt)
        {
            throw new NotImplementedException();
        }
    }
}