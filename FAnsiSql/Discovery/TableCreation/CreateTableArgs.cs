using System.Collections.Generic;
using System.Data;
using System.Globalization;
using TypeGuesser;

namespace FAnsi.Discovery.TableCreation;

/// <summary>
/// Determines the behaviour of <see cref="IDiscoveredDatabaseHelper.CreateTable"/>.  This includes how columns are assigned data types, whether foreign keys
/// are created etc.
/// </summary>
/// <remarks>
/// Create a table with the given name.  Set your columns in <see cref="ExplicitColumnDefinitions"/>
/// </remarks>
public sealed class CreateTableArgs(DiscoveredDatabase database, string tableName, string? schema)
{
    /// <summary>
    /// The destination database in which to create the table
    /// </summary>
    public DiscoveredDatabase Database { get; set; } = database;

    /// <summary>
    /// Name you want the table to have once created
    /// </summary>
    public string TableName { get; private set; } = tableName;

    /// <summary>
    /// Schema of the <see cref="Database"/> to create the table in.  This is NOT the database e.g. in [MyDb].[dbo].[MyTable] the schema is "dbo". If in doubt leave blank
    /// </summary>
    public string Schema { get; private set; } = schema;

    /// <summary>
    /// Optional - Columns are normally created based on supplied DataTable data rows.  If this is set then the Type specified here will
    /// be used instead.
    /// </summary>
    public DatabaseColumnRequest[]? ExplicitColumnDefinitions { get; set; }

    /// <summary>
    /// Set this to make last minute changes to column datatypes before table creation
    /// </summary>
    public IDatabaseColumnRequestAdjuster? Adjuster { get; set; }

    /// <summary>
    /// Link between columns that you want to create in your table <see cref="DatabaseColumnRequest"/> and existing columns (<see cref="DiscoveredColumn"/>) that
    /// should be paired with a foreign key constraint.
    ///
    /// Key is the foreign key column (and the table the constraint will be put on).
    /// Value is the primary key table column (which the constraint reference points to)
    /// </summary>
    public Dictionary<DatabaseColumnRequest, DiscoveredColumn> ForeignKeyPairs { get; set; }

    /// <summary>
    /// When creating a foreign key constraint (See <see cref="ForeignKeyPairs"/>) determines whether ON DELETE CASCADE should be set.
    /// </summary>
    public bool CascadeDelete { get; set; }

    /// <summary>
    /// The data to use to determine table schema and load into the newly created table (unless <see cref="CreateEmpty"/> is set).
    /// </summary>
    public DataTable DataTable { get; set; }

    /// <summary>
    /// When creating the table, do not upload any rows supplied in <see cref="DataTable"/>
    /// </summary>
    public bool CreateEmpty { get;  set; }

    /// <summary>
    /// True if the table has been created
    /// </summary>
    public bool TableCreated { get; private set; }

    /// <summary>
    /// Customise guessing behaviour
    /// </summary>
    public GuessSettings GuessSettings { get; set; } = GuessSettingsFactory.Create();

    /// <summary>
    /// Populated after the table has been created (See <see cref="TableCreated"/>), list of the <see cref="Guesser"/> used to create the columns in the table.
    /// <para>This will be null if no <see cref="DataTable"/> was provided when creating the table</para>
    /// </summary>
    public Dictionary<string, Guesser> ColumnCreationLogic { get; private set; }

    /// <summary>
    /// Used to determine what how to parse untyped strings in <see cref="DataTable"/> (if building schema from data table).
    /// </summary>
    public CultureInfo Culture { get; set; } = CultureInfo.CurrentCulture;

    /// <summary>
    /// Create a table with the given name.  Set your columns in <see cref="ExplicitColumnDefinitions"/>
    /// </summary>
    public CreateTableArgs(DiscoveredDatabase database, string tableName, string? schema,
        Dictionary<DatabaseColumnRequest, DiscoveredColumn> foreignKeyPairs, bool cascadeDelete)
        : this(database, tableName, schema)
    {
        ForeignKeyPairs = foreignKeyPairs;
        CascadeDelete = cascadeDelete;
    }

    /// <summary>
    /// Create a table with the given name based on the columns and data in the provided <paramref name="dataTable"/>.  If you want to override the
    /// data type of a given column set <see cref="ExplicitColumnDefinitions"/>
    /// </summary>
    public CreateTableArgs(DiscoveredDatabase database, string tableName, string? schema, DataTable dataTable, bool createEmpty)
        : this(database, tableName, schema)
    {
        DataTable = dataTable;
        CreateEmpty = createEmpty;
    }

    /// <summary>
    /// Create a table with the given name based on the columns and data in the provided <paramref name="dataTable"/>.  If you want to override the
    /// data type of a given column set <see cref="ExplicitColumnDefinitions"/>
    /// </summary>
    public CreateTableArgs(DiscoveredDatabase database, string tableName, string schema,DataTable dataTable, bool createEmpty, Dictionary<DatabaseColumnRequest, DiscoveredColumn> foreignKeyPairs, bool cascadeDelete)
        : this(database, tableName, schema,dataTable,createEmpty)
    {
        ForeignKeyPairs = foreignKeyPairs;
        CascadeDelete = cascadeDelete;
    }

    /// <summary>
    /// Declare that the table has been created and the provided <paramref name="columnsCreated"/> were used to determine the column schema
    /// </summary>
    /// <param name="columnsCreated"></param>
    public void OnTableCreated(Dictionary<string, Guesser> columnsCreated)
    {
        ColumnCreationLogic = columnsCreated;
        TableCreated = true;
    }
}