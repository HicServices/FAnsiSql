using System.Data;
using System.Data.Common;
using System.Diagnostics;
using FAnsi.Discovery;

namespace FAnsi.Connections;

/// <inheritdoc/>
public sealed class ManagedConnection : IManagedConnection
{
    /// <inheritdoc/>
    public DbConnection Connection { get; private set; }

    /// <inheritdoc/>
    public DbTransaction Transaction { get; private set; }

    /// <inheritdoc/>
    public IManagedTransaction ManagedTransaction { get; private set; }

    /// <inheritdoc/>
    public bool CloseOnDispose { get; set; }

    internal ManagedConnection(DiscoveredServer discoveredServer, IManagedTransaction managedTransaction)
    {
        //get a new connection or use the existing one within the transaction
        Connection = discoveredServer.GetConnection(managedTransaction);

        //if there is a transaction, also store the transaction
        ManagedTransaction = managedTransaction;
        Transaction = managedTransaction?.Transaction;

        //if there isn't a transaction then we opened a new connection, so we had better remember to close it again
        if (managedTransaction != null) return;

        CloseOnDispose = true;
        Debug.Assert(Connection.State == ConnectionState.Closed);
        Connection.Open();
    }

    public ManagedConnection Clone()
    {
        return (ManagedConnection) MemberwiseClone();
    }

    /// <summary>
    /// Closes and disposes the DbConnection unless this class is part of an <see cref="IManagedTransaction"/>
    /// </summary>
    public void Dispose()
    {
        if (CloseOnDispose)
            Connection.Dispose();
    }
}