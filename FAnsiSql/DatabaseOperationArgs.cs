﻿using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using FAnsi.Connections;
using FAnsi.Discovery;

namespace FAnsi;

/// <summary>
/// Arguments for facilitating long running sql operations which the user/system might want to cancel mid way through.
/// </summary>
public sealed class DatabaseOperationArgs
{
    /// <summary>
    /// If using an ongoing connection/transaction.  Otherwise null.
    /// </summary>
    public IManagedTransaction? TransactionIfAny { get; set; }

    /// <summary>
    /// Time to allow <see cref="DbCommand"/> to run before cancelling (this is db timeout and doesn't affect <see cref="CancellationToken"/>)
    /// </summary>
    public int TimeoutInSeconds { get; set; }

    /// <summary>
    /// Optional, if provided all commands interacting with these args should cancel if the command was cancelled
    /// </summary>
    public CancellationToken CancellationToken;

    public DatabaseOperationArgs()
    {

    }
    public DatabaseOperationArgs(IManagedTransaction transactionIfAny, int timeoutInSeconds,
        CancellationToken cancellationToken)
    {
        TransactionIfAny = transactionIfAny;
        CancellationToken = cancellationToken;
        TimeoutInSeconds = timeoutInSeconds;
    }

    /// <summary>
    /// Sets the timeout and cancellation on <paramref name="cmd"/> then runs <see cref="DbCommand.ExecuteNonQueryAsync()"/> with the
    /// <see cref="CancellationToken"/> (if any) and blocks till the call completes.
    /// 
    /// </summary>
    /// <param name="cmd"></param>
    /// <exception cref="OperationCanceledException"></exception>
    public int ExecuteNonQuery(DbCommand cmd)
    {
        return Execute(cmd, ()=>cmd.ExecuteNonQueryAsync(CancellationToken));
    }
    /// <summary>
    /// Sets the timeout and cancellation on <paramref name="cmd"/> then runs <see cref="DbCommand.ExecuteScalar()"/> with the
    /// <see cref="CancellationToken"/> (if any) and blocks till the call completes.
    /// 
    /// </summary>
    /// <param name="cmd"></param>
    /// <exception cref="OperationCanceledException"></exception>
    public object? ExecuteScalar(DbCommand cmd)
    {
        return Execute(cmd, ()=>cmd.ExecuteScalarAsync(CancellationToken));
    }

    private T Execute<T>(DbCommand cmd, Func<Task<T>> method)
    {
        Hydrate(cmd);
        var t = method();

        try
        {
            switch (t.Status)
            {
                case TaskStatus.Faulted:
                    throw t.Exception?? new Exception("Task crashed without Exception!");
                case TaskStatus.Canceled:
                    throw new OperationCanceledException();
                default:
                    t.Wait();
                    break;
            }
        }
        catch (AggregateException e)
        {
            if (e.InnerExceptions.Count == 1)
                throw e.InnerExceptions[0];

            throw;
        }

        if (!t.IsCompleted)
            cmd.Cancel();

        if (t.Exception == null) return t.Result;

        if (t.Exception.InnerExceptions.Count == 1)
            throw t.Exception.InnerExceptions[0];

        throw t.Exception;
    }

    public void Fill(DbDataAdapter da, DbCommand cmd, DataTable dt)
    {
        Hydrate(cmd);

        CancellationToken.ThrowIfCancellationRequested();

        if(CancellationToken.CanBeCanceled)
            dt.RowChanged += ThrowIfCancelled;

        da.Fill(dt);
        CancellationToken.ThrowIfCancellationRequested();
    }

    private void ThrowIfCancelled(object sender, DataRowChangeEventArgs e)
    {
        CancellationToken.ThrowIfCancellationRequested();
    }

    private void Hydrate(DbCommand cmd)
    {
        cmd.CommandTimeout = TimeoutInSeconds;
        CancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Opens a new connection or passes back an existing opened connection (that matches
    /// <see cref="TransactionIfAny"/>).  This command should be wrapped in a using statement
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    public IManagedConnection GetManagedConnection(DiscoveredTable table) => GetManagedConnection(table.Database.Server);

    /// <summary>
    /// Opens a new connection or passes back an existing opened connection (that matches
    /// <see cref="TransactionIfAny"/>).  This command should be wrapped in a using statement
    /// </summary>
    /// <param name="database"></param>
    /// <returns></returns>
    public IManagedConnection GetManagedConnection(DiscoveredDatabase database) => GetManagedConnection(database.Server);

    /// <summary>
    /// Opens a new connection or passes back an existing opened connection (that matches
    /// <see cref="TransactionIfAny"/>).  This command should be wrapped in a using statement
    /// </summary>
    /// <param name="server"></param>
    /// <returns></returns>
    public IManagedConnection GetManagedConnection(DiscoveredServer server) => server.GetManagedConnection(TransactionIfAny);
}