namespace FAnsi.Discovery;

/// <summary>
/// The nature of a queryable asset on the server (e.g. table, view, table valued function)
/// </summary>
public enum TableType
{
    /// <summary>
    /// A persistent query (view) run on a table on demand
    /// </summary>
    View,

    /// <summary>
    /// A physical table on a database server
    /// </summary>
    Table,

    /// <summary>
    /// A proceedural function which returns rows based on 0 or more parameters and acts like a table (DBMS specific).
    /// </summary>
    TableValuedFunction
}