using System;
using System.Data.Common;
using System.Diagnostics;

namespace FAnsi.Connections;

/// <inheritdoc/>
public sealed class ManagedTransaction : IManagedTransaction
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

    private bool closed;

    /// <summary>
    /// Attempts to rollback the DbTransaction (swallowing any Exception) and closes/disposes the DbConnection
    /// </summary>
    public void AbandonAndCloseConnection()
    {
        if(closed)
            return;

        closed = true;

        try
        {
            Transaction.Rollback();
        }
        catch (Exception e)
        {
            Trace.WriteLine($"Transaction rollback failed during AbandonAndCloseConnection{e.Message}");
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
        if(closed)
            return;

        closed = true;

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