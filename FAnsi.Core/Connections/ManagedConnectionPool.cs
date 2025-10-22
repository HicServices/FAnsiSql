using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Threading;
using FAnsi.Discovery;

namespace FAnsi.Connections;

/// <summary>
/// Provides thread-local connection pooling for DiscoveredServer instances to eliminate
/// ephemeral connection churn. Maintains one long-lived connection per thread per server.
/// </summary>
internal static class ManagedConnectionPool
{
    /// <summary>
    /// Thread-local storage for connections, keyed by connection string.
    /// Each thread maintains its own set of connections to different servers.
    /// </summary>
    private static readonly ThreadLocal<ConcurrentDictionary<string, IManagedConnection>> _threadLocalConnections =
        new(() => new ConcurrentDictionary<string, IManagedConnection>(), trackAllValues: true);

    /// <summary>
    /// Gets a pooled managed connection for the specified server. Returns a thread-local long-lived
    /// connection if available, otherwise creates a new one.
    /// </summary>
    /// <param name="server">The discovered server to connect to</param>
    /// <param name="transaction">Optional transaction to use (if specified, bypasses pooling)</param>
    /// <returns>A managed connection that should not be disposed (CloseOnDispose = false)</returns>
    internal static IManagedConnection GetPooledConnection(DiscoveredServer server, IManagedTransaction? transaction = null)
    {
        // If we have a transaction, create a standard non-pooled connection directly (bypassing pool to avoid recursion)
        if (transaction != null)
            return new ManagedConnection(server, transaction);

        var connectionKey = server.Builder.ConnectionString;
        var threadConnections = _threadLocalConnections.Value;

        // Try to get existing connection for this server on this thread
        if (threadConnections != null && threadConnections.TryGetValue(connectionKey, out var existingConnection))
        {
            // Verify connection is still valid and not in a transaction
            // Cannot reuse connections with active transactions as they have uncommitted state
            if (existingConnection?.Connection.State == ConnectionState.Open &&
                existingConnection.Transaction == null &&
                server.Helper.IsConnectionAlive(existingConnection.Connection))
            {
                // Return a non-disposing wrapper
                var wrapper = existingConnection.Clone();
                wrapper.CloseOnDispose = false;
                return wrapper;
            }

            // Connection is dead or in transaction, remove it and dispose
            threadConnections.TryRemove(connectionKey, out _);
            try
            {
                existingConnection?.Connection?.Dispose();
            }
            catch
            {
                // Swallow disposal errors
            }
        }

        // Create new long-lived connection for this thread directly (bypassing GetManagedConnection to avoid recursion)
        var newConnection = new ManagedConnection(server, null);
        newConnection.CloseOnDispose = false; // Don't close on dispose - we manage the lifetime

        // Store it
        if (threadConnections != null)
            threadConnections[connectionKey] = newConnection;

        // Return a non-disposing wrapper
        var returnWrapper = newConnection.Clone();
        returnWrapper.CloseOnDispose = false;
        return returnWrapper;
    }

    /// <summary>
    /// Clears all pooled connections for the current thread.
    /// Useful for cleanup or when you want to force new connections.
    /// </summary>
    internal static void ClearCurrentThreadConnections()
    {
        var threadConnections = _threadLocalConnections.Value;
        if (threadConnections == null) return;

        foreach (var kvp in threadConnections)
        {
            try
            {
                if (kvp.Value?.Connection.State == ConnectionState.Open)
                {
                    kvp.Value.Connection.Close();
                    kvp.Value.Connection.Dispose();
                }
            }
            catch
            {
                // Swallow exceptions during cleanup
            }
        }

        threadConnections.Clear();
    }

    /// <summary>
    /// Clears all pooled connections across all threads.
    /// Should be called during application shutdown.
    /// </summary>
    internal static void ClearAllConnections()
    {
        if (_threadLocalConnections?.Values == null) return;

        foreach (var threadConnections in _threadLocalConnections.Values)
        {
            if (threadConnections == null) continue;

            foreach (var kvp in threadConnections)
            {
                try
                {
                    if (kvp.Value?.Connection.State == ConnectionState.Open)
                    {
                        kvp.Value.Connection.Close();
                        kvp.Value.Connection.Dispose();
                    }
                }
                catch
                {
                    // Swallow exceptions during cleanup
                }
            }

            threadConnections.Clear();
        }
    }
}
