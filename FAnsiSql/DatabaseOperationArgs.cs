using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FAnsi.Connections;

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

        /// <summary>
        /// Sets the timeout and cancellation on <paramref name="cmd"/> then runs <see cref="DbCommand.ExecuteNonQueryAsync()"/> with the
        /// <see cref="CancellationToken"/> (if any) and blocks till the call completes.
        /// 
        /// </summary>
        /// <param name="cmd"></param>
        /// <exception cref="OperationCanceledException"></exception>
        public int ExecuteNonQuery(DbCommand cmd)
        {
            cmd.CommandTimeout = TimeoutInSeconds;
            CancellationToken.ThrowIfCancellationRequested();
            
            var t = cmd.ExecuteNonQueryAsync(CancellationToken);
            
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
    }
}
