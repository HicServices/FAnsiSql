using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Naming;
using TypeGuesser;

namespace FAnsi.Discovery;

/// <summary>
/// Request to create a column in a DatabaseType agnostic manner.  This class exists to let you declare a field called X where the data type is wide enough
/// to store strings up to 10 characters long (For example) without having to worry that it is varchar(10) in SqlServer but varchar2(10) in Oracle.
/// 
/// <para>Type specification is defined in the DatabaseTypeRequest but can also be specified explicitly (e.g. 'varchar(10)').</para>
/// </summary>
public sealed class DatabaseColumnRequest(string columnName, DatabaseTypeRequest typeRequested, bool allowNulls = true)
    : ISupplementalColumnInformation, IHasRuntimeName
{
    /// <summary>
    /// The fixed string proprietary data type to use.  This overrides <see cref="TypeRequested"/> if specified.
    ///
    /// <para>See also <see cref="GetSQLDbType"/></para>
    /// </summary>
    public string ExplicitDbType { get; set; }


    public string ColumnName { get; set; } = columnName;

    /// <summary>
    /// The cross database platform type descriptor for the column e.g. 'able to store strings up to 18 in length'.
    /// 
    /// <para>This is ignored if you have specified an <see cref="ExplicitDbType"/></para>
    /// 
    /// <para>See also <see cref="GetSQLDbType"/></para>
    /// </summary>
    public DatabaseTypeRequest TypeRequested { get; set; } = typeRequested;

    /// <summary>
    /// True to create a column which is nullable
    /// </summary>
    public bool AllowNulls { get; set; } = allowNulls;

    /// <summary>
    /// True to include the column as part of the tables primary key
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// True to create a column with auto incrementing number values in this column (autonum / identity etc)
    /// </summary>
    public bool IsAutoIncrement { get; set; }

    /// <summary>
    /// Set to create a default constraint on the column which calls the given scalar function
    /// </summary>
    public MandatoryScalarFunctions Default { get; set; }

    /// <summary>
    /// Applies only if the <see cref="TypeRequested"/> is string based.  Setting this will override the default collation and specify
    /// a specific collation.  The value specified must be an installed collation supported by the DBMS
    /// </summary>
    public string Collation { get; set; }

    public DatabaseColumnRequest(string columnName, string explicitDbType, bool allowNulls = true) : this(columnName, (DatabaseTypeRequest)null, allowNulls)
    {
        ExplicitDbType = explicitDbType;
    }

    /// <summary>
    /// Returns <see cref="ExplicitDbType"/> if set or uses the <see cref="TypeTranslater"/> to generate a proprietary type name for <see cref="TypeRequested"/>
    /// </summary>
    /// <param name="typeTranslater"></param>
    /// <returns></returns>
    public string GetSQLDbType(ITypeTranslater typeTranslater) => ExplicitDbType??typeTranslater.GetSQLDBTypeForCSharpType(TypeRequested);

    public string GetRuntimeName() => ColumnName;
}