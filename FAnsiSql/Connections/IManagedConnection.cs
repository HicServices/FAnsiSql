using System;
using System.Data.Common;
using FAnsi.Discovery;

namespace FAnsi.Connections
{
    /// <summary>
    /// Wrapper for DbConnection and optional DbTransaction.
    /// </summary>
    public interface IManagedConnection : IDisposable
    {
        /// <summary>
        /// DbConnection being wrapped
        /// </summary>
        DbConnection Connection { get; }

        /// <summary>
        /// Optional - DbTransaction being wrapped if one has been started or null
        /// </summary>
        DbTransaction Transaction { get; }

        /// <summary>
        /// Optional - transaction being run (See <see cref="DiscoveredServer.BeginNewTransactedConnection"/>.  If this is not null then <see cref="Transaction"/> should also be not null.
        /// </summary>
        IManagedTransaction ManagedTransaction { get; }
    }
}