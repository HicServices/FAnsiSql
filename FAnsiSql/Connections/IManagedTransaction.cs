using System.Data.Common;

namespace FAnsi.Connections;

/// <summary>
/// Wrapper for DbTransaction that associates it with a specific DbConnection.  Helps simplify calls to information 
/// methods such as DiscoveredTable.GetRowCount etc during the middle of an ongoing database transaction
/// </summary>
public interface IManagedTransaction
{
    /// <summary>
    /// The DbConnection that the <see cref="Transaction"/> is running on
    /// </summary>
    DbConnection Connection { get; }

    /// <summary>
    /// The DbTransaction being wrapped
    /// </summary>
    DbTransaction Transaction { get; }

    /// <summary>
    /// Calls <see cref="DbTransaction.Rollback()"/> and closes/disposes the <see cref="Connection"/>
    /// </summary>
    void AbandonAndCloseConnection();

    /// <summary>
    /// Calls <see cref="DbTransaction.Commit"/> and closes/disposes the <see cref="Connection"/>
    /// </summary>
    void CommitAndCloseConnection();
}