using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using TypeGuesser;

namespace FAnsi.Discovery;

/// <summary>
/// Cross database type reference to a Column in a Table
/// </summary>
public class DiscoveredColumn : IHasFullyQualifiedNameToo,ISupplementalColumnInformation
{
    /// <summary>
    /// The <see cref="DiscoveredTable"/> on which the <see cref="DiscoveredColumn"/> was found
    /// </summary>
    public DiscoveredTable Table { get; private set; }

    /// <summary>
    /// Stateless helper class with DBMS specific implementation of the logic required by <see cref="DiscoveredColumn"/>.
    /// </summary>
    public IDiscoveredColumnHelper Helper;

    /// <summary>
    /// True if the column allows rows with nulls in this column
    /// </summary>
    public bool AllowNulls { get; private set; }

    /// <summary>
    /// True if the column is part of the <see cref="Table"/> primary key (a primary key can consist of mulitple columns)
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// True if the column is an auto incrementing default number e.g. IDENTITY.  This will not handle roundabout ways of declaring
    /// auto increment e.g. sequences in Oracle, DEFAULT constraints etc.
    /// </summary>
    public bool IsAutoIncrement { get; set; }

    /// <summary>
    /// The DBMS proprietary column specific collation e.g. "Latin1_General_CS_AS_KS_WS"
    /// </summary>
    public string Collation { get; set; }

    /// <summary>
    /// The data type of the column found (includes String Length and Scale/Precision).
    /// </summary>
    public DiscoveredDataType DataType { get; set; }

    /// <summary>
    /// The character set of the column (if char)
    /// </summary>
    public string Format { get; set; }

    private readonly string _name;
    private readonly IQuerySyntaxHelper _querySyntaxHelper;

    /// <summary>
    /// Internal API constructor intended for Implementation classes, instead use <see cref="DiscoveredTable.DiscoverColumn"/> instead.
    /// </summary>
    /// <param name="table"></param>
    /// <param name="name"></param>
    /// <param name="allowsNulls"></param>
    public DiscoveredColumn(DiscoveredTable table, string name,bool allowsNulls)
    {
        Table = table;
        Helper = table.Helper.GetColumnHelper();

        _name = name;
        _querySyntaxHelper = table.Database.Server.GetQuerySyntaxHelper();
        AllowNulls = allowsNulls;
    }

    /// <summary>
    /// The unqualified name of the column e.g. "MyCol"
    /// </summary>
    /// <returns></returns>
    public string GetRuntimeName()
    {
        return _querySyntaxHelper.GetRuntimeName(_name);
    }

    /// <summary>
    /// The fully qualified name of the column e.g. [MyDb].dbo.[MyTable].[MyCol] or `MyDb`.`MyCol`
    /// </summary>
    /// <returns></returns>
    public string GetFullyQualifiedName()
    {
        return _querySyntaxHelper.EnsureFullyQualified(Table.Database.GetRuntimeName(),Table.Schema, Table.GetRuntimeName(), GetRuntimeName(), Table is DiscoveredTableValuedFunction);
    }


    /// <summary>
    /// Returns the SQL code required to fetch the <paramref name="topX"/> values from the table
    /// </summary>
    /// <param name="topX">The number of records to return</param>
    /// <param name="discardNulls">If true adds a WHERE statement to throw away null values</param>
    /// <returns></returns>
    public string GetTopXSql(int topX, bool discardNulls)
    {
        return Helper.GetTopXSqlForColumn(Table.Database, Table, this, topX, discardNulls);
    }

    /// <summary>
    /// Returns the name of the column
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return _name;
    }

    /// <summary>
    /// Generates a <see cref="Guesser"/> primed with the <see cref="DataType"/> of this column.  This can be used to inspect new
    /// untyped (string) data to determine whether it will fit into the column.
    /// </summary>
    /// <returns></returns>
    public Guesser GetGuesser()
    {
        return Table.GetQuerySyntaxHelper().TypeTranslater.GetGuesserFor(this);
    }

    /// <summary>
    /// Based on column name and Table
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    protected bool Equals(DiscoveredColumn other)
    {
        return string.Equals(_name, other._name) && Equals(Table, other.Table);
    }
    /// <summary>
    /// Based on column name and Table
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((DiscoveredColumn)obj);
    }

    /// <summary>
    /// Based on column name and Table
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return ((_name?.GetHashCode() ?? 0) * 397) ^ (Table?.GetHashCode() ?? 0);
        }
    }

    /// <summary>
    /// Returns the wrapped e.g. "[MyCol]" name of the column including escaping e.g. if you wanted to name a column "][nquisitor" (which would return "[]][nquisitor]").  Use <see cref="GetFullyQualifiedName()"/> to return the full name including table/database/schema.
    /// </summary>
    /// <returns></returns>
    public string GetWrappedName()
    {
        return Table.GetQuerySyntaxHelper().EnsureWrapped(GetRuntimeName());
    }
}