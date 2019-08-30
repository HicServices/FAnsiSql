using System;
using System.Data.Common;

namespace FAnsi.Connections
{
    /// <inheritdoc/>
    public class ManagedTransaction : IManagedTransaction
    {
        /// <inheritdoc/>
        public DbConnection Connection { get; private set; }

        /// <inheritdoc/>
        public DbTransaction Transaction { get; private set; }

        internal ManagedTransaction(DbConnection connection, DbTransaction transaction)
        {
            Connection = connection;
            Transaction = transaction;
        }

        /// <summary>
        /// Attempts to rollback the DbTransaction (swallowing any Exception) and closes/disposes the DbConnection
        /// </summary>
        public void AbandonAndCloseConnection()
        {
            try
            {
                Transaction.Rollback();
            }
            catch (Exception)
            {
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }

        /// <summary>
        /// Attempts to commit the DbTransaction and then closes/disposes the DbConnection
        /// </summary>
        public void CommitAndCloseConnection()
        {

            try
            {
                Transaction.Commit();
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }
    }
}
