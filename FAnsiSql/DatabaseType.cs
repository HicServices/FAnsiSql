namespace FAnsi;

/// <summary>
/// Describes a specific DBMS implementation you are talking to
/// </summary>
public enum DatabaseType
{
    /// <summary>
    /// Any Microsoft Sql Server database (e.g. Express etc).  Does not include Access.
    /// </summary>
    MicrosoftSQLServer,

    /// <summary>
    /// My Sql database engine.  
    /// </summary>
    MySql,

    /// <summary>
    /// Oracle database engine
    /// </summary>
    Oracle,

    /// <summary>
    /// PostgreSql database engine
    /// </summary>
    PostgreSql
}