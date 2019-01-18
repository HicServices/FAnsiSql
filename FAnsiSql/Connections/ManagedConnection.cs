﻿using System.Data;
using System.Data.Common;
using System.Diagnostics;
using FAnsi.Discovery;

namespace FAnsi.Connections
{
    /// <inheritdoc/>
    public class ManagedConnection : IManagedConnection
    {
        /// <inheritdoc/>
        public DbConnection Connection { get; private set; }

        /// <inheritdoc/>
        public DbTransaction Transaction { get; private set; }

        /// <inheritdoc/>
        public IManagedTransaction ManagedTransaction { get; private set; }
        
        private readonly bool _hadToOpenConnection = false;

        internal ManagedConnection(DiscoveredServer discoveredServer, IManagedTransaction managedTransaction)
        {
            //get a new connection or use the existing one within the transaction
            Connection = discoveredServer.GetConnection(managedTransaction);

            //if there is a transaction, also store the transaction
            ManagedTransaction = managedTransaction;
            Transaction = managedTransaction != null ? managedTransaction.Transaction : null;

            //if there isn't a transaction then we opened a new connection so we had better remember to close it again
            if(managedTransaction == null)
            {
                _hadToOpenConnection = true;
                Debug.Assert(Connection.State == ConnectionState.Closed);
                Connection.Open();
            }
        }

        /// <summary>
        /// Closes and disposes the DbConnection unless this class is part of an <see cref="IManagedTransaction"/>
        /// </summary>
        public void Dispose()
        {
            if (_hadToOpenConnection)
                Connection.Dispose();
        }
    }
}