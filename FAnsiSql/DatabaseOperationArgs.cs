using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FAnsi.Connections;
using FAnsi.Discovery;

namespace FAnsi
{
    /// <summary>
    /// Arguments for facilitating long running sql operations which the user/system might want to cancel mid way through.
    /// </summary>
    public class DatabaseOperationArgs
    {
        /// <summary>
        /// If using an ongoing connection/transaction.  Otherwise null.
        /// </summary>
        public IManagedTransaction TransactionIfAny{ get; set; }

        /// <summary>
        /// Time to allow <see cref="DbCommand"/> to run before cancelling (this is db timeout and doesn't affect <see cref="CancellationToken"/>)
        /// </summary>
        public int TimeoutInSeconds { get; set; }
        
        /// <summary>
        /// Optional, if provided all commands interacting with these args should cancel if the command was cancelled
        /// </summary>
        public CancellationToken CancellationToken =  default(CancellationToken);

        public DatabaseOperationArgs()
        {
            
        }
        public DatabaseOperationArgs(IManagedTransaction transactionIfAny, CancellationToken cancellationToken, int timeoutInSeconds)
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
            return Execute<int>(cmd, ()=>cmd.ExecuteNonQueryAsync(CancellationToken));
        }
        /// <summary>
        /// Sets the timeout and cancellation on <paramref name="cmd"/> then runs <see cref="DbCommand.ExecuteScalar()"/> with the
        /// <see cref="CancellationToken"/> (if any) and blocks till the call completes.
        /// 
        /// </summary>
        /// <param name="cmd"></param>
        /// <exception cref="OperationCanceledException"></exception>
        public object ExecuteScalar(DbCommand cmd)
        {
            return Execute<object>(cmd, ()=>cmd.ExecuteScalarAsync(CancellationToken));
        }

        private T Execute<T>(DbCommand cmd, Func<Task<T>> method)
        {
            Hydrate(cmd);
            var t = method();
            
            try
            {
                if (t.Status == TaskStatus.Faulted)
                    throw t.Exception?? new Exception("Task crashed without Exception!");
                
                if(t.Status != TaskStatus.Canceled)
                    t.Wait();
                else
                    throw new OperationCanceledException();
                
            }
            catch (AggregateException e)
            {
                if (e.InnerExceptions.Count == 1)
                    throw e.InnerExceptions[0];
                
                throw;
            }
            
            if (!t.IsCompleted) 
                cmd.Cancel();
            
            if( t.Exception != null)
                if (t.Exception.InnerExceptions.Count == 1)
                    throw t.Exception.InnerExceptions[0];
                else
                    throw t.Exception;

            return t.Result;
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
        public IManagedConnection GetManagedConnection(DiscoveredTable table)
        {
            return GetManagedConnection(table.Database.Server);
        }

        /// <summary>
        /// Opens a new connection or passes back an existing opened connection (that matches
        /// <see cref="TransactionIfAny"/>).  This command should be wrapped in a using statement
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public IManagedConnection GetManagedConnection(DiscoveredDatabase database)
        {
            return GetManagedConnection(database.Server);
        }

        /// <summary>
        /// Opens a new connection or passes back an existing opened connection (that matches
        /// <see cref="TransactionIfAny"/>).  This command should be wrapped in a using statement
        /// </summary>
        /// <param name="server"></param>
        /// <returns></returns>
        public IManagedConnection GetManagedConnection(DiscoveredServer server)
        {
            return server.GetManagedConnection(TransactionIfAny);
        }

    }
}
