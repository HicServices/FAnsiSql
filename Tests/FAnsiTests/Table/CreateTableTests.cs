using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Extensions;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table
{
    class CreateTableTests:DatabaseTests
    {
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
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

        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
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

        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
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
                default:
                    throw new ArgumentOutOfRangeException("type");
            }


            table.Drop();

            Assert.IsFalse(table.Exists());
        }

        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        public void CreateTable_PrimaryKey_FromDataTable(DatabaseType databaseType)
        {
            DiscoveredDatabase database = GetTestDatabase(databaseType);

            var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.PrimaryKey = new[] { dt.Columns[0] };
            dt.Rows.Add("Frank");

            DiscoveredTable table = database.CreateTable("PkTable", dt);

            Assert.IsTrue(table.DiscoverColumn("Name").IsPrimaryKey);
        }
        
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        public void CreateTable_PrimaryKey_FromColumnRequest(DatabaseType databaseType)
        {
            var database = GetTestDatabase(databaseType);

            var table = database.CreateTable(
                "PkTable",
                new DatabaseColumnRequest[]
                {
                    new DatabaseColumnRequest("Name",new DatabaseTypeRequest(typeof(string),10))
                        {
                            IsPrimaryKey = true
                        }
                });

            Assert.IsTrue(table.DiscoverColumn("Name").IsPrimaryKey);

            table.Drop();
        }

        [TestCase(DatabaseType.MicrosoftSQLServer, "Latin1_General_CS_AS_KS_WS")]
        [TestCase(DatabaseType.MySql, "latin1_german1_ci")]
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

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void CreateTable_BoolStrings(DatabaseType type)
        {
            var db = GetTestDatabase(type);
            DataTable dt = new DataTable();
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

        [Test]
        public void Test_OracleBit_IsActuallyString()
        {
            DiscoveredDatabase db = GetTestDatabase(DatabaseType.Oracle);
            DiscoveredTable table = db.CreateTable("MyTable",
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

            DataTable dt = new DataTable();
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

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MySql)]
        public void Test_CreateTable_UnicodeNames(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);

            DataTable dt = new DataTable();
            dt.Columns.Add("微笑");
            dt.Rows.Add("50");     

            var table = db.CreateTable("你好", dt);

            Assert.IsTrue(table.Exists());
            Assert.AreEqual("你好",table.GetRuntimeName());

            Assert.IsTrue(db.ExpectTable("你好").Exists());

            var col = table.DiscoverColumn("微笑");
            Assert.AreEqual("微笑", col.GetRuntimeName());

            table.Insert(new Dictionary<string, object>()
            {{ "微笑","10" } });
            
            Assert.AreEqual(2, table.GetRowCount());

            table.Insert(new Dictionary<DiscoveredColumn, object>()
            {{ col,"11" } });

            Assert.AreEqual(3,table.GetRowCount());

            var dt2 = new DataTable();
            dt2.Columns.Add("微笑");
            dt2.Rows.Add(23);

            using(var bulk = table.BeginBulkInsert())
                bulk.Upload(dt2);
            
            Assert.AreEqual(4,table.GetRowCount());
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        //[TestCase(DatabaseType.Oracle)] // Oracle doesn't really support bits https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns
        [TestCase(DatabaseType.MySql)]
        public void Test_CreateTable_TF(DatabaseType dbType)
        {
            //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
            var db = GetTestDatabase(dbType);
            DataTable dt = new DataTable();
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

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)] 
        [TestCase(DatabaseType.MySql)]
        public void Test_CreateTable_DoNotRetype(DatabaseType dbType)
        {
            //T and F is normally True and False.  If you want to keep it as a string set DoNotRetype
            var db = GetTestDatabase(dbType);
            DataTable dt = new DataTable();
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
            DataTable dt = new DataTable();
            dt.Columns.Add("C1");

            //the default Type for a DataColumn is string
            Assert.AreEqual(typeof(string),dt.Columns[0].DataType);

            dt.Columns["C1"].ExtendedProperties.Add("ff",true);

            var dt2 = dt.Clone();
            Assert.IsTrue(dt2.Columns["C1"].ExtendedProperties.ContainsKey("ff"));
        }

        [Test]
        public void Test_GetDoNotRetype_OnlyStringColumns()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("C1",typeof(int));
        
            dt.SetDoNotReType(true);

            //do not retype only applies when it is a string
            Assert.IsFalse(dt.Columns[0].GetDoNotReType());

            dt.Columns[0].DataType = typeof(string);

            //change it to a string and it applies
            Assert.IsTrue(dt.Columns[0].GetDoNotReType());
            
        }
    }
}
