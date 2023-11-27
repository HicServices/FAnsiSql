using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TableCreation;
using FAnsi.Extensions;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using TypeGuesser;

namespace FAnsiTests.Table;

internal class CreateTableTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateSimpleTable_Exists(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("People", new[]
        {
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10))
        });

        Assert.That(table.Exists());

        table.Drop();

        Assert.That(table.Exists(), Is.False);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestTableCreation(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var tbl = database.ExpectTable("CreatedTable");

        if (tbl.Exists())
            tbl.Drop();

        var syntaxHelper = database.Server.GetQuerySyntaxHelper();

        database.CreateTable(tbl.GetRuntimeName(), new[]
        {
            new DatabaseColumnRequest("name", new DatabaseTypeRequest(typeof(string),10), false){IsPrimaryKey=true},
            new DatabaseColumnRequest("foreignName", new DatabaseTypeRequest(typeof(string),7)){IsPrimaryKey=true},
            new DatabaseColumnRequest("address", new DatabaseTypeRequest(typeof (string), 500)),
            new DatabaseColumnRequest("dob", new DatabaseTypeRequest(typeof (DateTime)),false),
            new DatabaseColumnRequest("score",
                new DatabaseTypeRequest(typeof (decimal), null, new DecimalSize(5, 3))) //<- e.g. 12345.123

        });

        Assert.That(tbl.Exists());

        var colsDictionary = tbl.DiscoverColumns().ToDictionary(k => k.GetRuntimeName(), v => v, StringComparer.InvariantCultureIgnoreCase);

        var name = colsDictionary["name"];
        Assert.Multiple(() =>
        {
            Assert.That(name.DataType.GetLengthIfString(), Is.EqualTo(10));
            Assert.That(name.AllowNulls, Is.EqualTo(false));
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(name.DataType.SQLType), Is.EqualTo(typeof(string)));
            Assert.That(name.IsPrimaryKey);
        });

        var normalisedName = syntaxHelper.GetRuntimeName("foreignName"); //some database engines don't like capital letters?
        var foreignName = colsDictionary[normalisedName];
        Assert.Multiple(() =>
        {
            Assert.That(foreignName.AllowNulls, Is.EqualTo(false));//because it is part of the primary key we ignored the users request about nullability
            Assert.That(foreignName.DataType.GetLengthIfString(), Is.EqualTo(7));
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(foreignName.DataType.SQLType), Is.EqualTo(typeof(string)));
            Assert.That(foreignName.IsPrimaryKey);
        });

        var address = colsDictionary["address"];
        Assert.Multiple(() =>
        {
            Assert.That(address.DataType.GetLengthIfString(), Is.EqualTo(500));
            Assert.That(address.AllowNulls, Is.EqualTo(true));
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(address.DataType.SQLType), Is.EqualTo(typeof(string)));
            Assert.That(address.IsPrimaryKey, Is.False);
        });

        var dob = colsDictionary["dob"];
        Assert.Multiple(() =>
        {
            Assert.That(dob.DataType.GetLengthIfString(), Is.EqualTo(-1));
            Assert.That(dob.AllowNulls, Is.EqualTo(false));
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(dob.DataType.SQLType), Is.EqualTo(typeof(DateTime)));
            Assert.That(dob.IsPrimaryKey, Is.False);
        });

        var score = colsDictionary["score"];
        Assert.Multiple(() =>
        {
            Assert.That(score.AllowNulls, Is.EqualTo(true));
            Assert.That(score.DataType.GetDecimalSize().NumbersBeforeDecimalPlace, Is.EqualTo(5));
            Assert.That(score.DataType.GetDecimalSize().NumbersAfterDecimalPlace, Is.EqualTo(3));

            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(score.DataType.SQLType), Is.EqualTo(typeof(decimal)));
        });

        tbl.Drop();
    }


    [Test]
    public void Test_CreateTable_ProprietaryType()
    {
        var database = GetTestDatabase(DatabaseType.Oracle);

        var table = database.CreateTable(
            "MyTable",
            new [] {new DatabaseColumnRequest("Name", "VARCHAR2(10)")}
        );

        Assert.That(table.DiscoverColumn("Name").DataType.GetLengthIfString(), Is.EqualTo(10));

        table.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateSimpleTable_VarcharTypeCorrect(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("People", new[]
        {
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 5))
        });

        Assert.That(table.Exists());


        var dbType = table.DiscoverColumn("Name").DataType.SQLType;

        switch (type)
        {
            case DatabaseType.MicrosoftSQLServer:
                Assert.That(dbType, Is.EqualTo("varchar(5)"));
                break;
            case DatabaseType.MySql:
                Assert.That(dbType, Is.EqualTo("varchar(5)"));
                break;
            case DatabaseType.Oracle:
                Assert.That(dbType, Is.EqualTo("varchar2(5)"));
                break;
            case DatabaseType.PostgreSql:
                Assert.That(dbType, Is.EqualTo("character varying(5)"));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }


        table.Drop();

        Assert.That(table.Exists(), Is.False);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_PrimaryKey_FromDataTable(DatabaseType databaseType)
    {
        var database = GetTestDatabase(databaseType);

        var dt = new DataTable();
        dt.Columns.Add("Name");
        dt.PrimaryKey = new[] { dt.Columns[0] };
        dt.Rows.Add("Frank");

        var table = database.CreateTable("PkTable", dt);

        Assert.That(table.DiscoverColumn("Name").IsPrimaryKey);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_PrimaryKey_FromColumnRequest(DatabaseType databaseType)
    {
        var database = GetTestDatabase(databaseType);

        var table = database.CreateTable(
            "PkTable",
            new DatabaseColumnRequest[]
            {
                new("Name",new DatabaseTypeRequest(typeof(string),10))
                {
                    IsPrimaryKey = true
                }
            });

        Assert.That(table.DiscoverColumn("Name").IsPrimaryKey);

        table.Drop();
    }

    [TestCase(DatabaseType.MicrosoftSQLServer, "Latin1_General_CS_AS_KS_WS")]
    [TestCase(DatabaseType.MySql, "latin1_german1_ci")]
    [TestCase(DatabaseType.PostgreSql,"de-DE-x-icu")]
    //[TestCase(DatabaseType.Oracle, "BINARY_CI")] //Requires 12.2+ oracle https://www.experts-exchange.com/questions/29102764/SQL-Statement-to-create-case-insensitive-columns-and-or-tables-in-Oracle.html
    public void CreateTable_CollationTest(DatabaseType type, string collation)
    {
        var database = GetTestDatabase(type);

        var tbl = database.CreateTable("MyTable", new[]
        {
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string),100))
            {
                AllowNulls = false,
                Collation = collation
            }
        });

        Assert.That(tbl.DiscoverColumn("Name").Collation, Is.EqualTo(collation));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_BoolStrings(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.TableName = "MyTable";
        dt.Columns.Add("MyBoolCol",typeof(bool));
        dt.Rows.Add("true");

        var tbl = db.CreateTable("MyTable", dt);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.GetRowCount(), Is.EqualTo(1));

            /*if (type == DatabaseType.Oracle)
            {
                //Oracle doesn't have a bit datatype
                Assert.AreEqual(typeof(string), tbl.DiscoverColumn("MyBoolCol").GetGuesser().Guess.CSharpType);
                Assert.AreEqual("true", tbl.GetDataTable().Rows[0][0]);
                return;
            }*/

            Assert.That(tbl.DiscoverColumn("MyBoolCol").GetGuesser().Guess.CSharpType, Is.EqualTo(typeof(bool)));
            Assert.That(tbl.GetDataTable().Rows[0][0], Is.EqualTo(true));
        });
    }

    [Test]
    public void OracleRaceCondition()
    {
        var db = GetTestDatabase(DatabaseType.Oracle);

        var tbl = db.CreateTable("RaceTable", new[]
        {
            new DatabaseColumnRequest("A", "int"),
            new DatabaseColumnRequest("B", "int"),
            new DatabaseColumnRequest("C", "int"),
            new DatabaseColumnRequest("D", "int"),
            new DatabaseColumnRequest("E", "int")
        });

        Assert.That(tbl.GetDataTable().Columns, Has.Count.EqualTo(5));

        tbl.DropColumn(tbl.DiscoverColumn("E"));

        Assert.That(tbl.GetDataTable().Columns, Has.Count.EqualTo(4));

        tbl.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_DropColumn(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var tbl = db.CreateTable("RaceTable", new[]
        {
            new DatabaseColumnRequest("A", "int"){IsPrimaryKey = true},
            new DatabaseColumnRequest("B", "int")

        });

        Assert.That(tbl.GetDataTable().Columns, Has.Count.EqualTo(2));

        tbl.DropColumn(tbl.DiscoverColumn("B"));

        Assert.That(tbl.GetDataTable().Columns, Has.Count.EqualTo(1));

        tbl.Drop();
    }

    [Test]
    public void Test_OracleBit_IsNotStringAnyMore()
    {
        var db = GetTestDatabase(DatabaseType.Oracle);
        var table = db.CreateTable("MyTable",
            new[]
            {
                new DatabaseColumnRequest("MyCol", new DatabaseTypeRequest(typeof(bool)))
            });

        var col = table.DiscoverColumn("MyCol");
        Assert.That(col.DataType.SQLType, Is.EqualTo("decimal(1,0)"));
    }



    [TestCase(DatabaseType.MicrosoftSQLServer, "didn’t")] //<- it's a ’ not a '
    [TestCase(DatabaseType.MicrosoftSQLServer, "Æther")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "乗")]
    [TestCase(DatabaseType.Oracle, "didn’t")]
    [TestCase(DatabaseType.Oracle,"Æther")]
    [TestCase(DatabaseType.Oracle, "乗")]
    //[TestCase(DatabaseType.MySql, "didn’t")]
    //[TestCase(DatabaseType.MySql, "Æther")]
    //[TestCase(DatabaseType.MySql,"乗")]
    public void Test_CreateTable_UnicodeStrings(DatabaseType type,string testString)
    {
        var db = GetTestDatabase(type);

        var dt = new DataTable();
        dt.Columns.Add("Yay");
        dt.Rows.Add(testString);

        var table = db.CreateTable("GoGo",dt);

        //find the table column created
        var col = table.DiscoverColumn("Yay");

        //value fetched from database should match the one inserted
        var dbValue = (string) table.GetDataTable().Rows[0][0];
        Assert.That(dbValue, Is.EqualTo(testString));
        table.Drop();

        //column created should know it is unicode
        var typeRequest = col.Table.GetQuerySyntaxHelper().TypeTranslater.GetDataTypeRequestForSQLDBType(col.DataType.SQLType);
        Assert.That(typeRequest.Unicode, "Expected column DatabaseTypeRequest generated from column SQLType to be Unicode");

        //Column created should use unicode when creating a new datatype computer from the col
        var comp = col.GetGuesser();
        Assert.That(comp.Guess.Unicode);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_CreateTable_UnicodeNames(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var dt = new DataTable();
        dt.Columns.Add("微笑");
        dt.Rows.Add("50");

        var table = db.CreateTable("你好", dt);

        Assert.Multiple(() =>
        {
            Assert.That(table.Exists());
            Assert.That(table.GetRuntimeName(), Is.EqualTo("你好"));

            Assert.That(db.ExpectTable("你好").Exists());
        });

        var col = table.DiscoverColumn("微笑");
        Assert.That(col.GetRuntimeName(), Is.EqualTo("微笑"));

        table.Insert(new Dictionary<string, object> {{ "微笑","10" } });

        Assert.That(table.GetRowCount(), Is.EqualTo(2));

        table.Insert(new Dictionary<DiscoveredColumn, object> {{ col,"11" } });

        Assert.That(table.GetRowCount(), Is.EqualTo(3));

        using var dt2 = new DataTable();
        dt2.Columns.Add("微笑");
        dt2.Rows.Add(23);

        using(var bulk = table.BeginBulkInsert())
            bulk.Upload(dt2);

        Assert.That(table.GetRowCount(), Is.EqualTo(4));
    }

    [TestCase(DatabaseType.MicrosoftSQLServer)]
    //[TestCase(DatabaseType.Oracle)]\r\n // Oracle doesn't really support bits https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns
    [TestCase(DatabaseType.MySql)]
    [TestCase(DatabaseType.PostgreSql)]
    public void Test_CreateTable_TF(DatabaseType dbType)
    {
        //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
        var db = GetTestDatabase(dbType);
        var dt = new DataTable();
        dt.Columns.Add("Hb");
        dt.Rows.Add("T");
        dt.Rows.Add("F");

        var tbl = db.CreateTable("T1", dt);

        Assert.That(tbl.DiscoverColumn("Hb").DataType.GetCSharpDataType(), Is.EqualTo(typeof(bool)));

        var dt2 = tbl.GetDataTable();
        Assert.That(dt2.Rows.Cast<DataRow>().Select(c => c[0]).ToArray(), Does.Contain(true));
        Assert.That(dt2.Rows.Cast<DataRow>().Select(c => c[0]).ToArray(), Does.Contain(false));

        tbl.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_CreateTable_DoNotRetype(DatabaseType dbType)
    {
        //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
        var db = GetTestDatabase(dbType);
        using var dt = new DataTable();
        dt.Columns.Add("Hb");
        dt.Rows.Add("T");
        dt.Rows.Add("F");

        //do not retype string to bool
        dt.Columns["Hb"].SetDoNotReType(true);

        var tbl = db.CreateTable("T1", dt);

        Assert.That(tbl.DiscoverColumn("Hb").DataType.GetCSharpDataType(), Is.EqualTo(typeof(string)));

        var dt2 = tbl.GetDataTable();
        var values = dt2.Rows.Cast<DataRow>().Select(static c => (string)c[0]).ToArray();
        Assert.That(values, Does.Contain("T"));
        Assert.That(values, Does.Contain("F"));

        tbl.Drop();
    }

    /// <summary>
    /// Just to check that clone on <see cref="DataTable"/> properly clones <see cref="DataColumn.ExtendedProperties"/>
    /// </summary>
    [Test]
    public void Test_DataTableClone_ExtendedProperties()
    {
        using var dt = new DataTable();
        dt.Columns.Add("C1");

        //the default Type for a DataColumn is string
        Assert.That(dt.Columns[0].DataType, Is.EqualTo(typeof(string)));

        dt.Columns["C1"]?.ExtendedProperties.Add("ff",true);

        var dt2 = dt.Clone();
        Assert.That(dt2.Columns["C1"]?.ExtendedProperties.ContainsKey("ff")??false);
    }

    [Test]
    public void Test_GetDoNotRetype_OnlyStringColumns()
    {
        using var dt = new DataTable();
        dt.Columns.Add("C1",typeof(int));

        dt.SetDoNotReType(true);

        //do not retype only applies when it is a string
        Assert.That(dt.Columns[0].GetDoNotReType(), Is.False);

        dt.Columns[0].DataType = typeof(string);

        //change it to a string and it applies
        Assert.That(dt.Columns[0].GetDoNotReType());
    }

    /// <summary>
    /// Tests how CreateTable interacts with <see cref="DataColumn"/> of type Object
    /// </summary>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_ObjectColumns_StringContent(DatabaseType dbType)
    {
        //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
        var db = GetTestDatabase(dbType);
        var dt = new DataTable();
        dt.Columns.Add("Hb",typeof(object));
        dt.Rows.Add("T");
        dt.Rows.Add("F");

        var ex = Assert.Throws<NotSupportedException>(()=>db.CreateTable("T1", dt));

        StringAssert.Contains("System.Object",ex?.Message);

    }

    /// <summary>
    /// Tests how we can customize how "T" and "F" etc are interpreted (either as boolean true/false or as string). This test
    /// uses the static defaults in <see cref="GuessSettingsFactory.Defaults"/>.
    /// </summary>
    [TestCase(DatabaseType.MicrosoftSQLServer,true)]
    [TestCase(DatabaseType.MicrosoftSQLServer,false)]
    public void CreateTable_GuessSettings_StaticDefaults_TF(DatabaseType dbType, bool treatAsBoolean)
    {
        //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
        var db = GetTestDatabase(dbType);
        using var dt = new DataTable();
        dt.Columns.Add("Hb");
        dt.Rows.Add("T");
        dt.Rows.Add("F");

        var initialDefault = GuessSettingsFactory.Defaults.CharCanBeBoolean;

        try
        {
            //change the static default option
            GuessSettingsFactory.Defaults.CharCanBeBoolean = treatAsBoolean;

            var tbl = db.CreateTable("T1", dt);
            var col = tbl.DiscoverColumn("Hb");

            Assert.Multiple(() =>
            {
                Assert.That(col.DataType.GetCSharpDataType(), Is.EqualTo(treatAsBoolean ? typeof(bool) : typeof(string)));
                Assert.That(col.DataType.GetLengthIfString(), Is.EqualTo(treatAsBoolean ? -1 : 1), "Expected string length to be 1 for 'T'");
            });
        }
        finally
        {
            GuessSettingsFactory.Defaults.CharCanBeBoolean = initialDefault;
        }
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestSomething(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);


        var tbl = db.CreateTable("ScriptsRun", new[]
        {
            new DatabaseColumnRequest("cint", new DatabaseTypeRequest(typeof(int)))
                {IsAutoIncrement = true, IsPrimaryKey = true},
            new DatabaseColumnRequest("clong", new DatabaseTypeRequest(typeof(long))),
            new DatabaseColumnRequest("cshort", new DatabaseTypeRequest(typeof(short))),
            new DatabaseColumnRequest("script_name", new DatabaseTypeRequest(typeof(string), 255)),
            new DatabaseColumnRequest("text_of_script", new DatabaseTypeRequest(typeof(string), int.MaxValue)),
            new DatabaseColumnRequest("text_hash", new DatabaseTypeRequest(typeof(string), 512) {Unicode = true}),
            new DatabaseColumnRequest("one_time_script", new DatabaseTypeRequest(typeof(bool))),
            new DatabaseColumnRequest("entry_date", new DatabaseTypeRequest(typeof(DateTime))),
            new DatabaseColumnRequest("modified_date", new DatabaseTypeRequest(typeof(DateTime))),
            new DatabaseColumnRequest("entered_by", new DatabaseTypeRequest(typeof(string), 50))

        });

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists());

            Assert.That(tbl.DiscoverColumn("cint").DataType.GetCSharpDataType(), Is.EqualTo(typeof(int)));
            Assert.That(tbl.DiscoverColumn("clong").DataType.GetCSharpDataType(), Is.EqualTo(typeof(long)));
            Assert.That(tbl.DiscoverColumn("cshort").DataType.GetCSharpDataType(), Is.EqualTo(typeof(short)));
            Assert.That(tbl.DiscoverColumn("script_name").DataType.GetCSharpDataType(), Is.EqualTo(typeof(string)));
        });
        tbl.Drop();
    }

    /// <summary>
    /// Tests how we can customize how "T" and "F" etc are interpreted (either as boolean true/false or as string). This test
    /// uses the <see cref="CreateTableArgs.GuessSettings"/> injection.
    /// </summary>
    [TestCase(DatabaseType.MicrosoftSQLServer,true)]
    [TestCase(DatabaseType.MicrosoftSQLServer,false)]
    public void CreateTable_GuessSettings_InArgs_TF(DatabaseType dbType, bool treatAsBoolean)
    {
        //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
        var db = GetTestDatabase(dbType);
        var dt = new DataTable();
        dt.Columns.Add("Hb");
        dt.Rows.Add("T");
        dt.Rows.Add("F");

        var args = new CreateTableArgs(db,"Hb",null,dt,false);
        Assert.Multiple(() =>
        {
            Assert.That(GuessSettingsFactory.Defaults.CharCanBeBoolean, Is.EqualTo(args.GuessSettings.CharCanBeBoolean), "Default should match the static default");
            Assert.That(args.GuessSettings, Is.Not.EqualTo(GuessSettingsFactory.Defaults), "Args should not be the same instance! otherwise we would unintentionally edit the defaults!");
        });

        //change the args settings
        args.GuessSettings.CharCanBeBoolean = treatAsBoolean;

        var tbl = db.CreateTable(args);
        var col = tbl.DiscoverColumn("Hb");

        Assert.Multiple(() =>
        {
            Assert.That(col.DataType.GetCSharpDataType(), Is.EqualTo(treatAsBoolean ? typeof(bool) : typeof(string)));
            Assert.That(col.DataType.GetLengthIfString(), Is.EqualTo(treatAsBoolean ? -1 : 1), "Expected string length to be 1 for 'T'");
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void CreateTable_GuessSettings_ExplicitDateTimeFormat(DatabaseType dbType, bool useCustomDate)
    {
        //Values like 013020 would normally be treated as string data (due to leading zero) but maybe the user wants it to be a date?
        var db = GetTestDatabase(dbType);
        var dt = new DataTable();
        dt.Columns.Add("DateCol");
        dt.Rows.Add("013020");

        var args = new CreateTableArgs(db,"Hb",null,dt,false);
        Assert.Multiple(() =>
        {
            Assert.That(GuessSettingsFactory.Defaults.ExplicitDateFormats, Is.EqualTo(args.GuessSettings.ExplicitDateFormats), "Default should match the static default");
            Assert.That(args.GuessSettings, Is.Not.EqualTo(GuessSettingsFactory.Defaults), "Args should not be the same instance! otherwise we would unintentionally edit the defaults!");
        });

        //change the args settings to treat this date format
        args.GuessSettings.ExplicitDateFormats = useCustomDate ? new[]{"MMddyy" } :null;

        var tbl = db.CreateTable(args);
        var col = tbl.DiscoverColumn("DateCol");

        Assert.That(col.DataType.GetCSharpDataType(), Is.EqualTo(useCustomDate ? typeof(DateTime): typeof(string)));

        var dtDown = tbl.GetDataTable();
        Assert.That(dtDown.Rows[0][0], Is.EqualTo(useCustomDate ? new DateTime(2020,01,30): "013020"));
    }
    [Test]
    public void GuessSettings_CopyProperties()
    {
        var props = typeof(GuessSettings).GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.SetProperty).Select(p => p.Name).ToArray();
        Assert.That(props, Has.Length.EqualTo(2), "There are new settable Properties in GuessSettings, we should copy them across in DiscoveredDatabaseHelper.CreateTable");
    }
}