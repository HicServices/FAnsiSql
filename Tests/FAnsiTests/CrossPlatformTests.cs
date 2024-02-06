using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsiTests.TypeTranslation;
using NUnit.Framework;
using TypeGuesser;
using TypeGuesser.Deciders;

namespace FAnsiTests;

public sealed class CrossPlatformTests:DatabaseTests
{

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestTableCreation_NullTableName(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        Assert.Throws<ArgumentNullException>(()=>db.CreateTable("", new DataTable()));
    }

    [TestCase(DatabaseType.MicrosoftSQLServer, "01/01/2007 00:00:00")]
    [TestCase(DatabaseType.MySql, "1/1/2007 00:00:00")]
    [TestCase(DatabaseType.MySql, "01/01/2007 00:00:00")]
    [TestCase(DatabaseType.Oracle, "01/01/2007 00:00:00")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "2007-01-01 00:00:00")]
    [TestCase(DatabaseType.MySql, "2007-01-01 00:00:00")]
    [TestCase(DatabaseType.Oracle, "2007-01-01 00:00:00")]
    [TestCase(DatabaseType.PostgreSql, "01/01/2007 00:00:00")]
    [TestCase(DatabaseType.PostgreSql,"2007-01-01 00:00:00")]
    public void DateColumnTests_NoTime(DatabaseType type, object input)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyTable",[new DatabaseColumnRequest("MyDate",new DatabaseTypeRequest(typeof(DateTime)))]);

        tbl.Insert(new Dictionary<string, object> { { "MyDate", input } });

        using (var blk = tbl.BeginBulkInsert())
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyDate");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedDate = new DateTime(2007, 1, 1);
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0][0], Is.EqualTo(expectedDate));
            Assert.That(result.Rows[1][0], Is.EqualTo(expectedDate));
        });
    }

    [TestCase(DatabaseType.MicrosoftSQLServer, "2/28/1993 5:36:27 AM","en-US")]
    [TestCase(DatabaseType.MySql, "2/28/1993 5:36:27 AM","en-US")]
    [TestCase(DatabaseType.Oracle, "2/28/1993 5:36:27 AM","en-US")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "28/2/1993 5:36:27 AM","en-GB")]
    [TestCase(DatabaseType.MySql, "28/2/1993 5:36:27 AM","en-GB")]
    [TestCase(DatabaseType.Oracle, "28/2/1993 5:36:27 AM","en-GB")]
    [TestCase(DatabaseType.PostgreSql,"2/28/1993 5:36:27 AM","en-US")]
    [TestCase(DatabaseType.PostgreSql,"28/2/1993 5:36:27 AM","en-GB")]
    public void DateColumnTests_UkUsFormat_Explicit(DatabaseType type, object input, string culture)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyTable",[new DatabaseColumnRequest("MyDate",new DatabaseTypeRequest(typeof(DateTime)))]);

        var cultureInfo = new CultureInfo (culture);

        //basic insert
        tbl.Insert(new Dictionary<string, object> { { "MyDate", input } },cultureInfo);

        //then bulk insert, both need to work
        using (var blk = tbl.BeginBulkInsert(cultureInfo))
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyDate");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedDate = new DateTime(1993, 2,28,5,36,27);
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0][0], Is.EqualTo(expectedDate));
            Assert.That(result.Rows[1][0], Is.EqualTo(expectedDate));
        });
    }


    /// <summary>
    /// Since DateTimes are converted in DataTable in memory before being up loaded to the database we need to check
    /// that any PrimaryKey on the <see cref="DataTable"/> is not compromised
    /// </summary>
    /// <param name="type"></param>
    /// <param name="input"></param>
    /// <param name="culture"></param>
    [TestCase(DatabaseType.MicrosoftSQLServer, "2/28/1993 5:36:27 AM","en-US")]
    [TestCase(DatabaseType.MySql, "2/28/1993 5:36:27 AM","en-US")]
    [TestCase(DatabaseType.Oracle, "2/28/1993 5:36:27 AM","en-US")]
    [TestCase(DatabaseType.PostgreSql, "2/28/1993 5:36:27 AM","en-US")]
    public void DateColumnTests_PrimaryKeyColumn(DatabaseType type, object input, string culture)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyTable",[
            new DatabaseColumnRequest("MyDate",new DatabaseTypeRequest(typeof(DateTime)))
                {IsPrimaryKey = true }
        ]);

        //then bulk insert, both need to work
        using (var blk = tbl.BeginBulkInsert(new CultureInfo(culture)))
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyDate");
            dt.Rows.Add(input);

            //this is the novel thing we are testing
            dt.PrimaryKey = [dt.Columns[0]];
            blk.Upload(dt);

            Assert.That(dt.PrimaryKey, Has.Length.EqualTo(1));
            Assert.That(dt.PrimaryKey[0].ColumnName, Is.EqualTo("MyDate"));
        }

        var result = tbl.GetDataTable();
        var expectedDate = new DateTime(1993, 2,28,5,36,27);
        Assert.That(result.Rows[0][0], Is.EqualTo(expectedDate));
    }


    [TestCase(DatabaseType.MicrosoftSQLServer, "00:00:00")]
    [TestCase(DatabaseType.MySql, "00:00:00")]
    [TestCase(DatabaseType.Oracle, "00:00:00")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "00:00")]
    [TestCase(DatabaseType.MySql, "00:00")]
    [TestCase(DatabaseType.Oracle, "00:00")]
    [TestCase(DatabaseType.PostgreSql, "00:00:00")]
    [TestCase(DatabaseType.PostgreSql, "00:00")]
    public void DateColumnTests_TimeOnly_Midnight(DatabaseType type, object input)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyTable", [new DatabaseColumnRequest("MyTime", new DatabaseTypeRequest(typeof(TimeSpan)))]);

        tbl.Insert(new Dictionary<string, object> { { "MyTime", input } });

        using (var blk = tbl.BeginBulkInsert())
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyTime");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedTime = new TimeSpan(0,0,0,0);

        var resultTimeSpans =
            //Oracle is a bit special it only stores whole dates then has server side settings about how much to return (like a format string)
            type == DatabaseType.Oracle
            ? new[] { (DateTime)result.Rows[0][0], (DateTime)result.Rows[1][0] }.Select(static dt => dt.TimeOfDay)
                .Cast<object>().ToArray()
            : [result.Rows[0][0], result.Rows[1][0]];

        Assert.Multiple(() =>
        {
            Assert.That(resultTimeSpans[0], Is.EqualTo(expectedTime));
            Assert.That(resultTimeSpans[1], Is.EqualTo(expectedTime));
        });
    }

    /*
    [Test]
    public void TestOracleTimespans()
    {
        var db = GetTestDatabase(DatabaseType.Oracle);

        using (var con = db.Server.GetConnection())
        {
            con.Open();

            var cmd = db.Server.GetCommand("CREATE TABLE FANSITESTS.TimeTable (time_of_day timestamp)", con);
            cmd.ExecuteNonQuery();


            var cmd2 = db.Server.GetCommand("INSERT INTO FANSITESTS.TimeTable (time_of_day) VALUES (:time_of_day)", con);
            
            var param = cmd2.CreateParameter();
            param.ParameterName = ":time_of_day";
            param.DbType = DbType.Time;
            param.Value = new DateTime(1,1,1,1, 1, 1);

            cmd2.Parameters.Add(param); 
            cmd2.ExecuteNonQuery();
            
            var tbl = db.ExpectTable("TimeTable");
            Assert.IsTrue(tbl.Exists());

            var result = tbl.GetDataTable();
            
            //Comes back as a DateTime, doesn't look like intervals are going to work either
            tbl.Drop();
        }
    }
    */
    [TestCase(DatabaseType.MicrosoftSQLServer, "13:11:10")]
    [TestCase(DatabaseType.MySql, "13:11:10")]
    [TestCase(DatabaseType.Oracle, "13:11:10")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "13:11")]
    [TestCase(DatabaseType.MySql, "13:11")]
    [TestCase(DatabaseType.Oracle, "13:11")]
    [TestCase(DatabaseType.PostgreSql, "13:11:10")]
    [TestCase(DatabaseType.PostgreSql, "13:11")]
    public void DateColumnTests_TimeOnly_Afternoon(DatabaseType type, object input)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyTable", [new DatabaseColumnRequest("MyTime", new DatabaseTypeRequest(typeof(TimeSpan)))]);

        tbl.Insert(new Dictionary<string, object> { { "MyTime", input } });

        using (var blk = tbl.BeginBulkInsert())
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyTime");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedTime = new TimeSpan(13,11,00);

        var resultTimeSpans =
            //Oracle is a bit special it only stores whole dates then has server side settings about how much to return (like a format string)
            type == DatabaseType.Oracle
            ? new[] { (DateTime)result.Rows[0][0], (DateTime)result.Rows[1][0] }.Select(static dt => dt.TimeOfDay)
                .Cast<object>().ToArray()
            : [result.Rows[0][0], result.Rows[1][0]];

        foreach (var t in resultTimeSpans.Cast<TimeSpan>())
        {
            if(t.Seconds>0)
                Assert.That(t.Seconds, Is.EqualTo(10));

            var eval = t.Subtract(new TimeSpan(0, 0, 0, t.Seconds));
            Assert.That(eval, Is.EqualTo(expectedTime));
        }
    }

    [Test]
    [TestCase(DatabaseType.MicrosoftSQLServer, "int", "-23.00")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "int", "23.0")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "bit", "0")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "int", "00.0")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "int", "-24")]
    [TestCase(DatabaseType.MySql, "int", "-23.00")]
    [TestCase(DatabaseType.MySql, "int", "-25")]
    [TestCase(DatabaseType.MySql, "bit", "0")]
    [TestCase(DatabaseType.PostgreSql, "int", "-23.00")]
    [TestCase(DatabaseType.PostgreSql, "int", "23.0")]
    [TestCase(DatabaseType.PostgreSql, "bit", "0")]
    [TestCase(DatabaseType.PostgreSql, "int", "00.0")]
    [TestCase(DatabaseType.PostgreSql, "int", "-24")]
    public void TypeConsensusBetweenGuesserAndDiscoveredTableTest(DatabaseType dbType, string datatType,string insertValue)
    {
        var database = GetTestDatabase(dbType);

        var tbl = database.ExpectTable("TestTableCreationStrangeTypology");

        if (tbl.Exists())
            tbl.Drop();

        var dt = new DataTable("TestTableCreationStrangeTypology");
        dt.Columns.Add("mycol");
        dt.Rows.Add(insertValue);

        var c = new Guesser();

        var tt = tbl.GetQuerySyntaxHelper().TypeTranslater;
        c.AdjustToCompensateForValue(insertValue);

        database.CreateTable(tbl.GetRuntimeName(),dt);

        Assert.That(c.GetSqlDBType(tt), Is.EqualTo(datatType));

        var expectedDataType = datatType;

        //you ask for an int PostgreSql gives you an integer!
        if(dbType == DatabaseType.PostgreSql)
            expectedDataType = datatType switch
            {
                "int" => "integer",
                "bit" => "bit(1)",
                _ => expectedDataType
            };

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("mycol").DataType.SQLType, Is.EqualTo(expectedDataType));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(1));
        });

        tbl.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void ForeignKeyCreationTest(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var tblParent = database.CreateTable("Parent",
        [
            new DatabaseColumnRequest("ID",new DatabaseTypeRequest(typeof(int))){IsPrimaryKey =  true},
            new DatabaseColumnRequest("Name",new DatabaseTypeRequest(typeof(string),10)) //varchar(10)
        ]);

        var parentIdPkCol = tblParent.DiscoverColumn("ID");

        var parentIdFkCol = new DatabaseColumnRequest("Parent_ID", new DatabaseTypeRequest(typeof (int)));

        var tblChild = database.CreateTable("Child",
        [
            parentIdFkCol,
            new DatabaseColumnRequest("ChildName",new DatabaseTypeRequest(typeof(string),10)) //varchar(10)
        ], new Dictionary<DatabaseColumnRequest, DiscoveredColumn>
        {
            {parentIdFkCol, parentIdPkCol}
        },true);
        try
        {
            using (var intoParent = tblParent.BeginBulkInsert())
            {
                using var dt = new DataTable();
                dt.Columns.Add("ID");
                dt.Columns.Add("Name");

                dt.Rows.Add(1, "Bob");
                dt.Rows.Add(2, "Frank");

                intoParent.Upload(dt);
            }

            using (var con = tblChild.Database.Server.GetConnection())
            {
                con.Open();

                var cmd = tblParent.Database.Server.GetCommand(
                    $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (100,'chucky')", con);

                //violation of fk
                Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception);

                tblParent.Database.Server.GetCommand(
                    $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,'chucky')", con).ExecuteNonQuery();
                tblParent.Database.Server.GetCommand(
                    $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,'chucky2')", con).ExecuteNonQuery();
            }

            Assert.Multiple(() =>
            {
                Assert.That(tblParent.GetRowCount(), Is.EqualTo(2));
                Assert.That(tblChild.GetRowCount(), Is.EqualTo(2));
            });

            using (var con = tblParent.Database.Server.GetConnection())
            {
                con.Open();

                var cmd = tblParent.Database.Server.GetCommand($"DELETE FROM {tblParent.GetFullyQualifiedName()}", con);
                cmd.ExecuteNonQuery();
            }

            Assert.Multiple(() =>
            {
                Assert.That(tblParent.GetRowCount(), Is.EqualTo(0));
                Assert.That(tblChild.GetRowCount(), Is.EqualTo(0));
            });
        }
        finally
        {
            tblChild.Drop();
            tblParent.Drop();
        }
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void ForeignKeyCreationTest_TwoColumns(DatabaseType type, bool cascadeDelete)
    {
        var database = GetTestDatabase(type);

        var tblParent = database.CreateTable("Parent",
        [
            new DatabaseColumnRequest("ID1",new DatabaseTypeRequest(typeof(int))){IsPrimaryKey =  true}, //varchar(10)
            new DatabaseColumnRequest("ID2",new DatabaseTypeRequest(typeof(int))){IsPrimaryKey =  true}, //varchar(10)
            new DatabaseColumnRequest("Name",new DatabaseTypeRequest(typeof(string),10)) //varchar(10)
        ]);

        var parentIdPkCol1 = tblParent.DiscoverColumn("ID1");
        var parentIdPkCol2 = tblParent.DiscoverColumn("ID2");

        var parentIdFkCol1 = new DatabaseColumnRequest("Parent_ID1", new DatabaseTypeRequest(typeof(int)));
        var parentIdFkCol2 = new DatabaseColumnRequest("Parent_ID2", new DatabaseTypeRequest(typeof(int)));

        var tblChild = database.CreateTable("Child",
        [
            parentIdFkCol1,
            parentIdFkCol2,
            new DatabaseColumnRequest("ChildName",new DatabaseTypeRequest(typeof(string),10)) //varchar(10)
        ], new Dictionary<DatabaseColumnRequest, DiscoveredColumn>
        {
            {parentIdFkCol1,parentIdPkCol1},
            {parentIdFkCol2,parentIdPkCol2}
        }, cascadeDelete);

        using (var intoParent = tblParent.BeginBulkInsert())
        {
            using var dt = new DataTable();
            dt.Columns.Add("ID1");
            dt.Columns.Add("ID2");
            dt.Columns.Add("Name");

            dt.Rows.Add(1,2, "Bob");

            intoParent.Upload(dt);
        }

        using (var con = tblChild.Database.Server.GetConnection())
        {
            con.Open();

            var cmd = tblParent.Database.Server.GetCommand(
                $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,3,'chucky')", con);

            //violation of fk
            Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception);

            tblParent.Database.Server.GetCommand(
                $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,2,'chucky')", con).ExecuteNonQuery();
            tblParent.Database.Server.GetCommand(
                $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,2,'chucky2')", con).ExecuteNonQuery();
        }

        Assert.Multiple(() =>
        {
            Assert.That(tblParent.GetRowCount(), Is.EqualTo(1));
            Assert.That(tblChild.GetRowCount(), Is.EqualTo(2));
        });

        using (var con = tblParent.Database.Server.GetConnection())
        {
            con.Open();
            var cmd = tblParent.Database.Server.GetCommand($"DELETE FROM {tblParent.GetFullyQualifiedName()}", con);

            if (cascadeDelete)
            {
                cmd.ExecuteNonQuery();
                Assert.Multiple(() =>
                {
                    Assert.That(tblParent.GetRowCount(), Is.EqualTo(0));
                    Assert.That(tblChild.GetRowCount(), Is.EqualTo(0));
                });
            }
            else
            {
                //no cascade deletes so the query should crash on violation of fk constraint
                Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception);
            }
        }
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateMaxVarcharColumns(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var tbl = database.CreateTable("TestDistincting",
        [
            new DatabaseColumnRequest("Field1",new DatabaseTypeRequest(typeof(string),int.MaxValue)), //varchar(max)
            new DatabaseColumnRequest("Field2",new DatabaseTypeRequest(typeof(string))), //varchar(???)
            new DatabaseColumnRequest("Field3",new DatabaseTypeRequest(typeof(string),1000)), //varchar(???)
            new DatabaseColumnRequest("Field4",new DatabaseTypeRequest(typeof(string),5000)), //varchar(???)
            new DatabaseColumnRequest("Field5",new DatabaseTypeRequest(typeof(string),10000)), //varchar(???)
            new DatabaseColumnRequest("Field6",new DatabaseTypeRequest(typeof(string),10)) //varchar(10)
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists());

            Assert.That(tbl.DiscoverColumn("Field1").DataType.GetLengthIfString(), Is.GreaterThanOrEqualTo(4000));
            Assert.That(tbl.DiscoverColumn("Field2").DataType.GetLengthIfString(), Is.GreaterThanOrEqualTo(1000)); // unknown size should be at least 1k? that seems sensible
            Assert.That(tbl.DiscoverColumn("Field6").DataType.GetLengthIfString(), Is.EqualTo(10));
        });
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateMaxVarcharColumnFromDataTable(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var dt = new DataTable();
        dt.Columns.Add("MassiveColumn");

        var sb = new StringBuilder("Amaa");
        for (var i = 0; i < 10000; i++)
            sb.Append(i);

        dt.Rows.Add(sb.ToString());


        var tbl=  database.CreateTable("MassiveTable", dt);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists());
            Assert.That(tbl.DiscoverColumn("MassiveColumn").DataType.GetLengthIfString(), Is.GreaterThanOrEqualTo(8000));
        });

        dt = tbl.GetDataTable();
        Assert.That(dt.Rows[0][0], Is.EqualTo(sb.ToString()));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateDateColumnFromDataTable(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var dt = new DataTable();
        dt.Columns.Add("DateColumn");
        dt.Rows.Add("2001-01-22");

        var tbl = database.CreateTable("DateTable", dt);

        Assert.That(tbl.Exists());

        dt = tbl.GetDataTable();
        Assert.That(dt.Rows[0][0], Is.EqualTo(new DateTime(2001,01,22)));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void AddColumnTest(DatabaseType type,bool useTransaction)
    {
        const string newColumnName = "My Fun New Column[Lol]"; //<- lets make sure dodgy names are also supported

        var database = GetTestDatabase(type);

        //create a single column table with primary key
        var tbl = database.CreateTable("TestDistincting",
        [
            new DatabaseColumnRequest("Field1",new DatabaseTypeRequest(typeof(string),100)){IsPrimaryKey = true} //varchar(max)
        ]);

        Assert.Multiple(() =>
        {

            //table should exist
            Assert.That(tbl.Exists());

            //column should be varchar(100)
            Assert.That(tbl.DiscoverColumn("Field1").DataType.GetLengthIfString(), Is.EqualTo(100));

            //and should be a primary key
            Assert.That(tbl.DiscoverColumn("Field1").IsPrimaryKey);
        });

        //ALTER TABLE to ADD COLUMN of date type
        if (useTransaction)
        {
            using var con = database.Server.BeginNewTransactedConnection();
            tbl.AddColumn(newColumnName, new DatabaseTypeRequest(typeof(DateTime)), true,new DatabaseOperationArgs{TimeoutInSeconds = 1000,TransactionIfAny = con.ManagedTransaction});
            con.ManagedTransaction.CommitAndCloseConnection();
        }
        else
        {
            tbl.AddColumn(newColumnName, new DatabaseTypeRequest(typeof(DateTime)), true, 1000);
        }


        //new column should exist
        var newCol = tbl.DiscoverColumn(newColumnName);

        //and should have a type of datetime as requested
        var typeCreated = newCol.DataType.SQLType;
        var tt = database.Server.GetQuerySyntaxHelper().TypeTranslater;
        Assert.That(tt.GetCSharpTypeForSQLDBType(typeCreated), Is.EqualTo(typeof(DateTime)));

        var fieldsToAlter = new List<string>(new []{"Field1", newColumnName});

        //sql server can't handle altering primary key columns or anything with a foreign key on it too!
        if (type == DatabaseType.MicrosoftSQLServer)
            fieldsToAlter.Remove("Field1");

        foreach (var fieldName in fieldsToAlter)
        {

            //ALTER TABLE, ALTER COLUMN of date type each of these to be now varchar(10)s

            //discover the column
            newCol = tbl.DiscoverColumn(fieldName);

            //ALTER the column to varchar(10)
            var newTypeCSharp = new DatabaseTypeRequest(typeof(string), 10);
            var newTypeSql = tt.GetSQLDBTypeForCSharpType(newTypeCSharp);
            newCol.DataType.AlterTypeTo(newTypeSql);

            //rediscover it
            newCol = tbl.DiscoverColumn(fieldName);

            //make sure the type change happened
            Assert.That(newCol.DataType.GetLengthIfString(), Is.EqualTo(10));
        }

        Assert.Multiple(() =>
        {
            //and should still be a primary key
            Assert.That(tbl.DiscoverColumn("Field1").IsPrimaryKey);
            //and should not be a primary key
            Assert.That(tbl.DiscoverColumn(newColumnName).IsPrimaryKey, Is.False);
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void ChangeDatabaseShouldNotAffectOriginalConnectionString_Test(DatabaseType type)
    {
        var database1 = GetTestDatabase(type);
        var stringBefore = database1.Server.Builder.ConnectionString;
        database1.Server.ExpectDatabase("SomeOtherDb");

        Assert.That(database1.Server.Builder.ConnectionString, Is.EqualTo(stringBefore));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithTwoBoolFlags))]
    public void TestDistincting(DatabaseType type,bool useTransaction, bool dodgyNames)
    {
        var database = GetTestDatabase(type);

        // JS 2023-05-11 4000 characters, because SELECT DISTINCT doesn't work on CLOB (Oracle)
        var tbl = database.CreateTable(dodgyNames?",,":"Field3",
        [
            new DatabaseColumnRequest("Field1",new DatabaseTypeRequest(typeof(string),4000)), //varchar(max)
            new DatabaseColumnRequest("Field2",new DatabaseTypeRequest(typeof(DateTime))),
            new DatabaseColumnRequest(dodgyNames?",,,,":"Field3",new DatabaseTypeRequest(typeof(int)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Field1");
        dt.Columns.Add("Field2");
        dt.Columns.Add(dodgyNames?",,,,":"Field3");

        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("frank", "2001-01-01", "50");
        dt.Rows.Add("frank", "2001-01-01", "50");
        dt.Rows.Add("frank", "2001-01-01", "51");

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
        });

        using (var insert = tbl.BeginBulkInsert())
            insert.Upload(dt);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(7));

        if(useTransaction)
        {
            using var con = tbl.Database.Server.BeginNewTransactedConnection();
            tbl.MakeDistinct(new DatabaseOperationArgs {TransactionIfAny = con.ManagedTransaction});
            con.ManagedTransaction.CommitAndCloseConnection();
        }
        else
            tbl.MakeDistinct();

        Assert.Multiple(() =>
        {
            Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
            Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestIntDataTypes(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var dt = new DataTable();
        dt.Columns.Add("MyCol",typeof(decimal));

        dt.Rows.Add("100");
        dt.Rows.Add("105");
        dt.Rows.Add("1");

        var tbl = database.CreateTable("IntTestTable", dt);

        dt = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToInt32(r[0]) == 100), Is.EqualTo(1));
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToInt32(r[0]) == 105), Is.EqualTo(1));
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToInt32(r[0]) == 1), Is.EqualTo(1));
        });

        var col = tbl.DiscoverColumn("MyCol");
        col.DataType.AlterTypeTo("decimal(5,2)");

        var size = tbl.DiscoverColumn("MyCol").DataType.GetDecimalSize();
        Assert.That(size, Is.EqualTo(new DecimalSize(3, 2))); //3 before decimal place 2 after;
        Assert.Multiple(() =>
        {
            Assert.That(size.NumbersBeforeDecimalPlace, Is.EqualTo(3));
            Assert.That(size.NumbersAfterDecimalPlace, Is.EqualTo(2));
            Assert.That(size.Precision, Is.EqualTo(5));
            Assert.That(size.Scale, Is.EqualTo(2));
        });

        dt = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToDecimal(r[0]) == new decimal(100.0f)), Is.EqualTo(1));
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToDecimal(r[0]) == new decimal(105.0f)), Is.EqualTo(1));
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToDecimal(r[0]) == new decimal(1.0f)), Is.EqualTo(1));
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestFloatDataTypes(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var dt = new DataTable();
        dt.Columns.Add("MyCol");

        dt.Rows.Add("100");
        dt.Rows.Add("105");
        dt.Rows.Add("2.1");

        var tbl = database.CreateTable("DecimalTestTable", dt);

        dt =tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToDecimal(r[0]) == new decimal(100.0f)), Is.EqualTo(1));
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToDecimal(r[0]) == new decimal(105.0f)), Is.EqualTo(1));
            Assert.That(dt.Rows.OfType<DataRow>().Count(static r => Convert.ToDecimal(r[0]) == new decimal(2.1f)), Is.EqualTo(1));
        });


        var col = tbl.DiscoverColumn("MyCol");
        var size = col.DataType.GetDecimalSize();
        Assert.That(size, Is.EqualTo(new DecimalSize(3, 1))); //3 before decimal place 2 after;
        Assert.Multiple(() =>
        {
            Assert.That(size.NumbersBeforeDecimalPlace, Is.EqualTo(3));
            Assert.That(size.NumbersAfterDecimalPlace, Is.EqualTo(1));
            Assert.That(size.Precision, Is.EqualTo(4));
            Assert.That(size.Scale, Is.EqualTo(1));
        });

        col.DataType.AlterTypeTo("decimal(5,2)");

        size = tbl.DiscoverColumn("MyCol").DataType.GetDecimalSize();
        Assert.That(size, Is.EqualTo(new DecimalSize(3,2))); //3 before decimal place 2 after;
        Assert.Multiple(() =>
        {
            Assert.That(size.NumbersBeforeDecimalPlace, Is.EqualTo(3));
            Assert.That(size.NumbersAfterDecimalPlace, Is.EqualTo(2));
            Assert.That(size.Precision, Is.EqualTo(5));
            Assert.That(size.Scale, Is.EqualTo(2));
        });
    }

    [TestCase(DatabaseType.MySql, "_-o-_",":>0<:")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "_-o-_", ":>0<:")]
    [TestCase(DatabaseType.PostgreSql, "_-o-_", ":>0<:")]
    public void HorribleDatabaseAndTableNames(DatabaseType type,string horribleDatabaseName, string horribleTableName)
    {
        AssertCanCreateDatabases();

        var database = GetTestDatabase(type);

        SqlConnection.ClearAllPools();

        database = database.Server.ExpectDatabase(horribleDatabaseName);
        database.Create(true);

        SqlConnection.ClearAllPools();

        try
        {
            var tbl = database.CreateTable(horribleTableName,
            [
                new DatabaseColumnRequest("Field1",new DatabaseTypeRequest(typeof(string),int.MaxValue)), //varchar(max)
                new DatabaseColumnRequest("Field2",new DatabaseTypeRequest(typeof(DateTime))),
                new DatabaseColumnRequest("Field3",new DatabaseTypeRequest(typeof(int))){AllowNulls=false}
            ]);

            using var dt = new DataTable();
            dt.Columns.Add("Field1");
            dt.Columns.Add("Field2");
            dt.Columns.Add("Field3");

            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("frank", "2001-01-01", "50");
            dt.Rows.Add("frank", "2001-01-01", "50");
            dt.Rows.Add("frank", "2001-01-01", "51");

            Assert.Multiple(() =>
            {
                Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
                Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
            });

            using (var insert = tbl.BeginBulkInsert())
                insert.Upload(dt);

            Assert.That(tbl.GetRowCount(), Is.EqualTo(7));

            tbl.MakeDistinct();

            Assert.Multiple(() =>
            {
                Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
                Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
            });

            tbl.Truncate();

            tbl.CreatePrimaryKey(tbl.DiscoverColumn("Field3"));

            Assert.That(tbl.DiscoverColumn("Field3").IsPrimaryKey);

        }
        finally
        {
            database.Drop();
        }
    }

    [TestCase(DatabaseType.MySql, "my (database)", "my (table)", "my (col)")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "my (database)", "my (table)", "my (col)")]
    [TestCase(DatabaseType.Oracle, "my (database)", "my (table)", "my (col)")]
    [TestCase(DatabaseType.MySql, "my.database", "my.table", "my.col")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "my.database", "my.table", "my.col")]
    [TestCase(DatabaseType.Oracle, "my.database", "my.table", "my.col")]
    [TestCase(DatabaseType.PostgreSql, "my (database)", "my (table)", "my (col)")]
    [TestCase(DatabaseType.PostgreSql, "my.database", "my.table", "my.col")]
    public void UnsupportedEntityNames(DatabaseType type, string horribleDatabaseName, string horribleTableName,string columnName)
    {

        var database = GetTestDatabase(type);

        Assert.Multiple(() =>
        {
            //ExpectDatabase with illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.Server.ExpectDatabase(horribleDatabaseName))
                    ?.Message, Does.Match("Database .* contained unsupported .* characters"));

            //ExpectTable with illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.ExpectTable(horribleTableName))
                    ?.Message, Does.Match("Table .* contained unsupported .* characters"));

            //CreateTable with illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.CreateTable(horribleTableName,
                [
                    new DatabaseColumnRequest("a", new DatabaseTypeRequest(typeof(string), 10))
                ]))
                    ?.Message, Does.Match("Table .* contained unsupported .* characters"));

            //CreateTable with (column) illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.CreateTable("f",
                [
                    new DatabaseColumnRequest(columnName, new DatabaseTypeRequest(typeof(string), 10))
                ]))
                    ?.Message, Does.Match("Column .* contained unsupported .* characters"));
        });

        AssertCanCreateDatabases();

        //CreateDatabase with illegal name
        Assert.That(
            Assert.Throws<RuntimeNameException>(()=>database.Server.CreateDatabase(horribleDatabaseName))
                ?.Message, Does.Match("Database .* contained unsupported .* characters"));
    }

    [TestCase(DatabaseType.MySql, "_-o-_", ":>0<:","-_")]
    [TestCase(DatabaseType.MySql, "Comment", "SSSS", "Space Out")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "_-o-_", ":>0<:", "-_")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "Comment", "SSSS", "Space Out")]
    [TestCase(DatabaseType.Oracle, "_-o-_", ":>0<:", "-_")]
    [TestCase(DatabaseType.Oracle, "Comment", "Comment", "Comment")] //reserved keyword in Oracle
    [TestCase(DatabaseType.Oracle, "Comment", "SSSS", "Space Out")]
    [TestCase(DatabaseType.PostgreSql, "_-o-_", ":>0<:", "-_")]
    [TestCase(DatabaseType.PostgreSql, "Comment", "Comment", "Comment")] //reserved keyword in Oracle
    [TestCase(DatabaseType.PostgreSql, "Comment", "SSSS", "Space Out")]
    public void HorribleColumnNames(DatabaseType type, string horribleDatabaseName, string horribleTableName,string columnName)
    {
        AssertCanCreateDatabases();

        var database = GetTestDatabase(type);

        database = database.Server.ExpectDatabase(horribleDatabaseName);
        database.Create(true);
        Assert.That(database.GetRuntimeName(), Is.EqualTo(horribleDatabaseName).IgnoreCase);

        try
        {
            var dt = new DataTable();
            dt.Columns.Add(columnName);
            dt.Rows.Add("dave");
            dt.PrimaryKey = [dt.Columns[0]];

            var tbl = database.CreateTable(horribleTableName, dt);

            Assert.Multiple(() =>
            {
                Assert.That(tbl.GetRowCount(), Is.EqualTo(1));

                Assert.That(tbl.DiscoverColumns().Single().IsPrimaryKey);

                Assert.That(tbl.GetDataTable().Rows, Has.Count.EqualTo(1));
            });

            tbl.Insert(new Dictionary<string, object> { {columnName,"fff" } });

            Assert.That(tbl.GetDataTable().Rows, Has.Count.EqualTo(2));
        }
        finally
        {
            database.Drop();
        }
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_AutoIncrementColumnTest(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var tbl =  database.CreateTable("MyTable",
        [
            new DatabaseColumnRequest("IdColumn", new DatabaseTypeRequest(typeof (int)))
            {
                AllowNulls = false,
                IsAutoIncrement = true,
                IsPrimaryKey = true
            },
            new DatabaseColumnRequest("Name",new DatabaseTypeRequest(typeof(string),100))

        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Name");
        dt.Rows.Add("Frank");

        using (var bulkInsert = tbl.BeginBulkInsert())
            bulkInsert.Upload(dt);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(1));

        var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["IdColumn"], Is.EqualTo(1));

            Assert.That(tbl.DiscoverColumn("IdColumn").IsAutoIncrement);
            Assert.That(tbl.DiscoverColumn("Name").IsAutoIncrement, Is.False);
        });

        var autoIncrement = tbl.Insert(new Dictionary<string, object> {{"Name", "Tony"}});
        Assert.That(autoIncrement, Is.EqualTo(2));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_DefaultTest_Date(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        var tbl = database.CreateTable("MyTable",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string),100)),
            new DatabaseColumnRequest("myDt", new DatabaseTypeRequest(typeof (DateTime)))
            {
                AllowNulls = false,
                Default = MandatoryScalarFunctions.GetTodaysDate
            }
        ]);
        DateTime currentValue;

        using (var insert = tbl.BeginBulkInsert())
        {
            using var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Rows.Add("Hi");

            currentValue = DateTime.Now;
            insert.Upload(dt);
        }

        var dt2 = tbl.GetDataTable();

        var databaseValue = (DateTime)dt2.Rows.Cast<DataRow>().Single()["myDt"];

        Assert.Multiple(() =>
        {
            Assert.That(databaseValue.Year, Is.EqualTo(currentValue.Year));
            Assert.That(databaseValue.Month, Is.EqualTo(currentValue.Month));
            Assert.That(databaseValue.Day, Is.EqualTo(currentValue.Day));
            Assert.That(databaseValue.Hour, Is.EqualTo(currentValue.Hour));
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_DefaultTest_Guid(DatabaseType type)
    {
        var database = GetTestDatabase(type);

        // ReSharper disable once SwitchStatementMissingSomeEnumCasesNoDefault
        switch (type)
        {
            case DatabaseType.MySql when database.Server.GetVersion().Major < 8:
                Assert.Inconclusive("UID defaults are only supported in MySql 8+");
                break;
            case DatabaseType.PostgreSql:
            {
                //we need this extension on the server to work
                using var con = database.Server.GetConnection();
                con.Open();
                using var cmd = database.Server.GetCommand("CREATE EXTENSION IF NOT EXISTS pgcrypto;", con);
                cmd.ExecuteNonQuery();
                break;
            }
        }

        var tbl = database.CreateTable("MyTable",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string),100)),
            new DatabaseColumnRequest("MyGuid", new DatabaseTypeRequest(typeof (string)))
            {
                AllowNulls = false,
                Default = MandatoryScalarFunctions.GetGuid
            }
        ]);

        using (var insert = tbl.BeginBulkInsert())
        {
            using var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Rows.Add("Hi");

            insert.Upload(dt);
        }

        var dt2 = tbl.GetDataTable();

        var databaseValue = (string)dt2.Rows.Cast<DataRow>().Single()["MyGuid"];

        Assert.That(databaseValue, Is.Not.Null);
        TestContext.WriteLine(databaseValue);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_BulkInserting_LotsOfDates(DatabaseType type)
    {
        var culture = new CultureInfo("en-gb");
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("LotsOfDatesTest",
        [
            new DatabaseColumnRequest("ID",new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("MyDate",new DatabaseTypeRequest(typeof(DateTime))),
            new DatabaseColumnRequest("MyString",new DatabaseTypeRequest(typeof(string),int.MaxValue))
        ]);

        //test basic insert
        foreach(var s in someDates)
            tbl.Insert(new Dictionary<string,object>
                {
                    {"ID",1},
                    {"MyDate",s},
                    {"MyString",Guid.NewGuid().ToString()}
                },culture
            );


        using var dt = new DataTable();

        dt.Columns.Add("id");
        dt.Columns.Add("mydate");
        dt.Columns.Add("mystring");

        foreach(var s in someDates)
            dt.Rows.Add(2,s,Guid.NewGuid().ToString());

        Assert.That(tbl.GetRowCount(), Is.EqualTo(someDates.Length));

        using(var bulkInsert = tbl.BeginBulkInsert(culture))
        {
            bulkInsert.Upload(dt);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(someDates.Length*2));
    }

    private readonly string [] someDates = [
        "22\\5\\19",
        "22/5/19",
        "22-5-19",
        "22.5.19",
        "Wed\\5\\19",
        "Wed/5/19",
        "Wed-5-19",
        "Wed.5.19",
        "Wednesday\\5\\19",
        "Wednesday/5/19",
        "Wednesday-5-19",
        "Wednesday.5.19",
        "22\\05\\19",
        "22/05/19",
        "22-05-19",
        "22.05.19",
        "Wed\\05\\19",
        "Wed/05/19",
        "Wed-05-19",
        "Wed.05.19",
        "Wednesday\\05\\19",
        "Wednesday/05/19",
        "Wednesday-05-19",
        "Wednesday.05.19",
        "22\\May\\19",
        "22/May/19",
        "22-May-19",
        "22.May.19",
        "Wed\\May\\19",
        "Wed/May/19",
        "Wed-May-19",
        "Wed.May.19",
        "Wednesday\\May\\19",
        "Wednesday/May/19",
        "Wednesday-May-19",
        "Wednesday.May.19",
        "22\\May\\19",
        "22/May/19",
        "22-May-19",
        "22.May.19",
        "Wed\\May\\19",
        "Wed/May/19",
        "Wed-May-19",
        "Wed.May.19",
        "Wednesday\\May\\19",
        "Wednesday/May/19",
        "Wednesday-May-19",
        "Wednesday.May.19",
        "22\\5\\2019",
        "22/5/2019",
        "22-5-2019",
        "22.5.2019",
        "Wed\\5\\2019",
        "Wed/5/2019",
        "Wed-5-2019",
        "Wed.5.2019",
        "Wednesday\\5\\2019",
        "Wednesday/5/2019",
        "Wednesday-5-2019",
        "Wednesday.5.2019",
        "22\\05\\2019",
        "22/05/2019",
        "22-05-2019",
        "22.05.2019",
        "Wed\\05\\2019",
        "Wed/05/2019",
        "Wed-05-2019",
        "Wed.05.2019",
        "Wednesday\\05\\2019",
        "Wednesday/05/2019",
        "Wednesday-05-2019",
        "Wednesday.05.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\5\\2019",
        "22/5/2019",
        "22-5-2019",
        "22.5.2019",
        "Wed\\5\\2019",
        "Wed/5/2019",
        "Wed-5-2019",
        "Wed.5.2019",
        "Wednesday\\5\\2019",
        "Wednesday/5/2019",
        "Wednesday-5-2019",
        "Wednesday.5.2019",
        "22\\05\\2019",
        "22/05/2019",
        "22-05-2019",
        "22.05.2019",
        "Wed\\05\\2019",
        "Wed/05/2019",
        "Wed-05-2019",
        "Wed.05.2019",
        "Wednesday\\05\\2019",
        "Wednesday/05/2019",
        "Wednesday-05-2019",
        "Wednesday.05.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\5\\02019",
        "22/5/02019",
        "22-5-02019",
        "22.5.02019",
        "Wed\\5\\02019",
        "Wed/5/02019",
        "Wed-5-02019",
        "Wed.5.02019",
        "Wednesday\\5\\02019",
        "Wednesday/5/02019",
        "Wednesday-5-02019",
        "Wednesday.5.02019",
        "22\\05\\02019",
        "22/05/02019",
        "22-05-02019",
        "22.05.02019",
        "Wed\\05\\02019",
        "Wed/05/02019",
        "Wed-05-02019",
        "Wed.05.02019",
        "Wednesday\\05\\02019",
        "Wednesday/05/02019",
        "Wednesday-05-02019",
        "Wednesday.05.02019",
        "22\\May\\02019",
        "22/May/02019",
        "22-May-02019",
        "22.May.02019",
        "Wed\\May\\02019",
        "Wed/May/02019",
        "Wed-May-02019",
        "Wed.May.02019",
        "Wednesday\\May\\02019",
        "Wednesday/May/02019",
        "Wednesday-May-02019",
        "Wednesday.May.02019",
        "22\\May\\02019",
        "22/May/02019",
        "22-May-02019",
        "22.May.02019",
        "Wed\\May\\02019",
        "Wed/May/02019",
        "Wed-May-02019",
        "Wed.May.02019",
        "Wednesday\\May\\02019",
        "Wednesday/May/02019",
        "Wednesday-May-02019",
        "Wednesday.May.02019",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM"
    ];



    [Test]
    public void DateTimeTypeDeciderPerformance()
    {
        var d = new DateTimeTypeDecider(new CultureInfo("en-gb"));
        var dt = new DateTime(2019,5,22,8,59,36);

        foreach(var f in DateTimeTypeDecider.DateFormatsDM) d.Parse(dt.ToString(f));
        foreach(var f in DateTimeTypeDecider.TimeFormats) d.Parse(dt.ToString(f));

        Assert.That(d.Parse("28/2/1993 5:36:27 AM"), Is.EqualTo(new DateTime(1993,2,28,5,36,27)));
    }
}