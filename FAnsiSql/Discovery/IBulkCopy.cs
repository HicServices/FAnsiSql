using System;
using System.Data;

namespace FAnsi.Discovery
{
    /// <summary>
    /// Cross database type implementation of Bulk Insert.  Each database API handles this differently, the differences are abstracted here through this
    /// interface such that the programmer doesn't need to know what type of database he is uploading a DataTable to in order for it to still work.
    /// </summary>
    public interface IBulkCopy : IDisposable
    {
        /// <summary>
        /// Upload all rows in the <paramref name="dt"/> to the destination table.
        /// </summary>
        /// <param name="dt"></param>
        /// <returns>number of rows written or number of rows affected (may not match row count if triggers etc are abound)</returns>
        int Upload(DataTable dt);

        /// <summary>
        /// The timeout in seconds for each call to <see cref="Upload"/> (implementation depends on derrived classes).
        /// </summary>
        int Timeout { get; set; }

        /// <summary>
        /// Notifies the <see cref="IBulkCopy"/> that the table schema has been changed mid insert! e.g. a column changing data type. This change must have taken place on the same
        /// DbTransaction as the bulkc copy.
        /// </summary>
        void InvalidateTableSchema();
    }
}