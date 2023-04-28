using System;
using System.Data.Common;
using FAnsi.Discovery;

namespace FAnsi.Connections;

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

    /// <summary>
    /// True to close the connection in the Dispose step.  If <see cref="IManagedConnection"/> opened the connection itself during construction then this flag will default
    /// to true otherwise it will default to false.
    /// </summary>
    bool CloseOnDispose { get; set; }

    /// <summary>
    /// Creates a new shallow copy instance of the <see cref="IManagedConnection"/>.  This will point to the same
    /// underlying <see cref="DbConnection"/> and <see cref="DbTransaction"/> (if any).
    /// </summary>
    /// <returns></returns>
    ManagedConnection Clone();
}