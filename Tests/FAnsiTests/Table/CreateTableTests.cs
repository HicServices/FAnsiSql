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

        Assert.IsTrue(table.Exists());

        table.Drop();

        Assert.IsFalse(table.Exists());
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

        Assert.IsTrue(tbl.Exists());

        var colsDictionary = tbl.DiscoverColumns().ToDictionary(k => k.GetRuntimeName(), v => v, StringComparer.InvariantCultureIgnoreCase);

        var name = colsDictionary["name"];
        Assert.AreEqual(10, name.DataType.GetLengthIfString());
        Assert.AreEqual(false, name.AllowNulls);
        Assert.AreEqual(typeof(string), syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(name.DataType.SQLType));
        Assert.IsTrue(name.IsPrimaryKey);

        var normalisedName = syntaxHelper.GetRuntimeName("foreignName"); //some database engines don't like capital letters?
        var foreignName = colsDictionary[normalisedName];
        Assert.AreEqual(false, foreignName.AllowNulls);//because it is part of the primary key we ignored the users request about nullability
        Assert.AreEqual(7, foreignName.DataType.GetLengthIfString());
        Assert.AreEqual(typeof(string), syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(foreignName.DataType.SQLType));
        Assert.IsTrue(foreignName.IsPrimaryKey);

        var address = colsDictionary["address"];
        Assert.AreEqual(500, address.DataType.GetLengthIfString());
        Assert.AreEqual(true, address.AllowNulls);
        Assert.AreEqual(typeof(string), syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(address.DataType.SQLType));
        Assert.IsFalse(address.IsPrimaryKey);

        var dob = colsDictionary["dob"];
        Assert.AreEqual(-1, dob.DataType.GetLengthIfString());
        Assert.AreEqual(false, dob.AllowNulls);
        Assert.AreEqual(typeof(DateTime), syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(dob.DataType.SQLType));
        Assert.IsFalse(dob.IsPrimaryKey);

        var score = colsDictionary["score"];
        Assert.AreEqual(true, score.AllowNulls);
        Assert.AreEqual(5, score.DataType.GetDecimalSize().NumbersBeforeDecimalPlace);
        Assert.AreEqual(3, score.DataType.GetDecimalSize().NumbersAfterDecimalPlace);

        Assert.AreEqual(typeof(decimal), syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(score.DataType.SQLType));

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

        Assert.AreEqual(10, table.DiscoverColumn("Name").DataType.GetLengthIfString());
            
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

        Assert.IsTrue(table.Exists());


        var dbType = table.DiscoverColumn("Name").DataType.SQLType;

        switch (type)
        {
            case DatabaseType.MicrosoftSQLServer:
                Assert.AreEqual("varchar(5)",dbType);
                break;
            case DatabaseType.MySql:
                Assert.AreEqual("varchar(5)",dbType);
                break;
            case DatabaseType.Oracle:
                Assert.AreEqual("varchar2(5)",dbType);
                break;
            case DatabaseType.PostgreSql:
                Assert.AreEqual("character varying(5)",dbType);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }


        table.Drop();

        Assert.IsFalse(table.Exists());
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

        Assert.IsTrue(table.DiscoverColumn("Name").IsPrimaryKey);
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

        Assert.IsTrue(table.DiscoverColumn("Name").IsPrimaryKey);

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

        Assert.AreEqual(collation, tbl.DiscoverColumn("Name").Collation);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_BoolStrings(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var dt = new DataTable();
        dt.TableName = "MyTable";
        dt.Columns.Add("MyBoolCol");
        dt.Rows.Add("true");

        var tbl = db.CreateTable("MyTable", dt);

        Assert.AreEqual(1,tbl.GetRowCount());

        if (type == DatabaseType.Oracle)
        {
            //Oracle doesn't have a bit datatype
            Assert.AreEqual(typeof(string), tbl.DiscoverColumn("MyBoolCol").GetGuesser().Guess.CSharpType);
            Assert.AreEqual("true", tbl.GetDataTable().Rows[0][0]);
            return;
        }

        Assert.AreEqual(typeof(bool),tbl.DiscoverColumn("MyBoolCol").GetGuesser().Guess.CSharpType);
        Assert.AreEqual(true,tbl.GetDataTable().Rows[0][0]);
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
            
        Assert.AreEqual(5,tbl.GetDataTable().Columns.Count);

        tbl.DropColumn(tbl.DiscoverColumn("E"));

        Assert.AreEqual(4, tbl.GetDataTable().Columns.Count);

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
            
        Assert.AreEqual(2,tbl.GetDataTable().Columns.Count);

        tbl.DropColumn(tbl.DiscoverColumn("B"));

        Assert.AreEqual(1, tbl.GetDataTable().Columns.Count);

        tbl.Drop();
    }

    [Test]
    public void Test_OracleBit_IsActuallyString()
    {
        var db = GetTestDatabase(DatabaseType.Oracle);
        var table = db.CreateTable("MyTable",
            new[]
            {
                new DatabaseColumnRequest("MyCol", new DatabaseTypeRequest(typeof(bool)))
            });

        var col = table.DiscoverColumn("MyCol");
        Assert.AreEqual("varchar2(5)", col.DataType.SQLType);
        Assert.AreEqual(5, col.DataType.GetLengthIfString());

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
        Assert.AreEqual(testString,dbValue);
        table.Drop();

        //column created should know it is unicode
        var typeRequest = col.Table.GetQuerySyntaxHelper().TypeTranslater.GetDataTypeRequestForSQLDBType(col.DataType.SQLType);
        Assert.IsTrue(typeRequest.Unicode, "Expected column DatabaseTypeRequest generated from column SQLType to be Unicode");

        //Column created should use unicode when creating a new datatype computer from the col
        var comp = col.GetGuesser();
        Assert.IsTrue(comp.Guess.Unicode);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_CreateTable_UnicodeNames(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var dt = new DataTable();
        dt.Columns.Add("微笑");
        dt.Rows.Add("50");     

        var table = db.CreateTable("你好", dt);

        Assert.IsTrue(table.Exists());
        Assert.AreEqual("你好",table.GetRuntimeName());

        Assert.IsTrue(db.ExpectTable("你好").Exists());

        var col = table.DiscoverColumn("微笑");
        Assert.AreEqual("微笑", col.GetRuntimeName());

        table.Insert(new Dictionary<string, object> {{ "微笑","10" } });
            
        Assert.AreEqual(2, table.GetRowCount());

        table.Insert(new Dictionary<DiscoveredColumn, object> {{ col,"11" } });

        Assert.AreEqual(3,table.GetRowCount());

        var dt2 = new DataTable();
        dt2.Columns.Add("微笑");
        dt2.Rows.Add(23);

        using(var bulk = table.BeginBulkInsert())
            bulk.Upload(dt2);
            
        Assert.AreEqual(4,table.GetRowCount());
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

        Assert.AreEqual(typeof(bool), tbl.DiscoverColumn("Hb").DataType.GetCSharpDataType());

        var dt2 = tbl.GetDataTable();
        Assert.Contains(true, dt2.Rows.Cast<DataRow>().Select(c => c[0]).ToArray());
        Assert.Contains(false, dt2.Rows.Cast<DataRow>().Select(c => c[0]).ToArray());
            
        tbl.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_CreateTable_DoNotRetype(DatabaseType dbType)
    {
        //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
        var db = GetTestDatabase(dbType);
        var dt = new DataTable();
        dt.Columns.Add("Hb");
        dt.Rows.Add("T");
        dt.Rows.Add("F");

        //do not retype string to bool
        dt.Columns["Hb"].SetDoNotReType(true);

        var tbl = db.CreateTable("T1", dt);

        Assert.AreEqual(typeof(string), tbl.DiscoverColumn("Hb").DataType.GetCSharpDataType());

        var dt2 = tbl.GetDataTable();
        Assert.Contains("T", dt2.Rows.Cast<DataRow>().Select(c => c[0]).ToArray());
        Assert.Contains("F", dt2.Rows.Cast<DataRow>().Select(c => c[0]).ToArray());

        tbl.Drop();
    }
        
    /// <summary>
    /// Just to check that clone on <see cref="DataTable"/> properly clones <see cref="DataColumn.ExtendedProperties"/>
    /// </summary>
    [Test]
    public void Test_DataTableClone_ExtendedProperties()
    {
        var dt = new DataTable();
        dt.Columns.Add("C1");

        //the default Type for a DataColumn is string
        Assert.AreEqual(typeof(string),dt.Columns[0].DataType);

        dt.Columns["C1"]?.ExtendedProperties.Add("ff",true);

        var dt2 = dt.Clone();
        Assert.IsTrue(dt2.Columns["C1"]?.ExtendedProperties.ContainsKey("ff"));
    }

    [Test]
    public void Test_GetDoNotRetype_OnlyStringColumns()
    {
        var dt = new DataTable();
        dt.Columns.Add("C1",typeof(int));
        
        dt.SetDoNotReType(true);

        //do not retype only applies when it is a string
        Assert.IsFalse(dt.Columns[0].GetDoNotReType());

        dt.Columns[0].DataType = typeof(string);

        //change it to a string and it applies
        Assert.IsTrue(dt.Columns[0].GetDoNotReType());
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

            Assert.AreEqual(treatAsBoolean ? typeof(bool): typeof(string),col.DataType.GetCSharpDataType());
            Assert.AreEqual(treatAsBoolean ? -1: 1,col.DataType.GetLengthIfString(),"Expected string length to be 1 for 'T'");
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

        Assert.IsTrue(tbl.Exists());

        Assert.AreEqual(typeof(int),tbl.DiscoverColumn("cint").DataType.GetCSharpDataType());
        Assert.AreEqual(typeof(long),tbl.DiscoverColumn("clong").DataType.GetCSharpDataType());
        Assert.AreEqual(typeof(short),tbl.DiscoverColumn("cshort").DataType.GetCSharpDataType());
        Assert.AreEqual(typeof(string),tbl.DiscoverColumn("script_name").DataType.GetCSharpDataType());
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
        Assert.AreEqual(args.GuessSettings.CharCanBeBoolean, GuessSettingsFactory.Defaults.CharCanBeBoolean,"Default should match the static default");
        Assert.IsFalse(args.GuessSettings == GuessSettingsFactory.Defaults,"Args should not be the same instance! otherwise we would unintentionally edit the defaults!");

        //change the args settings
        args.GuessSettings.CharCanBeBoolean = treatAsBoolean;
            
        var tbl = db.CreateTable(args);
        var col = tbl.DiscoverColumn("Hb");

        Assert.AreEqual(treatAsBoolean ? typeof(bool): typeof(string),col.DataType.GetCSharpDataType());
        Assert.AreEqual(treatAsBoolean ? -1: 1,col.DataType.GetLengthIfString(),"Expected string length to be 1 for 'T'");
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
        Assert.AreEqual(args.GuessSettings.ExplicitDateFormats, GuessSettingsFactory.Defaults.ExplicitDateFormats,"Default should match the static default");
        Assert.IsFalse(args.GuessSettings == GuessSettingsFactory.Defaults,"Args should not be the same instance! otherwise we would unintentionally edit the defaults!");

        //change the args settings to treat this date format
        args.GuessSettings.ExplicitDateFormats = useCustomDate ? new[]{"MMddyy" } :null;
            
        var tbl = db.CreateTable(args);
        var col = tbl.DiscoverColumn("DateCol");

        Assert.AreEqual(useCustomDate ? typeof(DateTime): typeof(string),col.DataType.GetCSharpDataType());

        var dtDown = tbl.GetDataTable();
        Assert.AreEqual(useCustomDate? new DateTime(2020,01,30): "013020" ,dtDown.Rows[0][0]);
    }
    [Test]
    public void GuessSettings_CopyProperties()
    {
        var props = typeof(GuessSettings).GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.SetProperty).Select(p => p.Name).ToArray();
        Assert.AreEqual(2,props.Length,"There are new settable Properties in GuessSettings, we should copy them across in DiscoveredDatabaseHelper.CreateTable");
    }
}