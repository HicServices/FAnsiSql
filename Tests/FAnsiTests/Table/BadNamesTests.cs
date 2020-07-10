using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TableCreation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using TypeGuesser;

namespace FAnsiTests.Table
{
    class BadNamesTests : DatabaseTests
    {
        /// <summary>
        /// It would be a bad idea to name your column this but if you really wanted to...
        /// </summary>
        const string BadColumnName = "Da'   ][\",,;ve";
        const string BadTableName = "Fi ; ][\"'`sh";
        const string BadColumnName2 = "Frrrrr ##' ank";
        private DiscoveredTable SetupBadNamesTable(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);

            return db.CreateTable(BadTableName,new[]
            {
                new DatabaseColumnRequest(BadColumnName,new DatabaseTypeRequest(typeof(string),100)), 
                new DatabaseColumnRequest(BadColumnName2,new DatabaseTypeRequest(typeof(int))) 
            });

        }

        [Test]
        public void BadNames_EnsureWrapped()
        {
            var db = GetTestDatabase(DatabaseType.MicrosoftSQLServer);
            var tbl = db.ExpectTable("][nquisitor");

            Assert.IsFalse(tbl.Exists());
            Assert.AreEqual("[]][nquisitor]",tbl.GetWrappedName());
        }

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void BadNames_DiscoverColumns(DatabaseType dbType)
        {
            var tbl = SetupBadNamesTable(dbType);
            var cols = tbl.DiscoverColumns();
            Assert.AreEqual(2,cols.Length);
            
            tbl.Drop();
        }

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void BadNames_AlterType(DatabaseType dbType)
        {
            var tbl = SetupBadNamesTable(dbType);
            
            var col = tbl.DiscoverColumn(BadColumnName);
            Assert.AreEqual(100,col.DataType.GetLengthIfString());

            string varcharType = tbl.Database.Server.GetQuerySyntaxHelper().TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string),10));
            
            // Can we ALTER it's datatype
            Assert.AreEqual(100,col.DataType.GetLengthIfString());
            col.DataType.AlterTypeTo(varcharType);
            Assert.AreEqual(10,col.DataType.GetLengthIfString());

            tbl.Drop();

        }

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
        public void BadNames_TopXColumn(DatabaseType dbType,bool noNulls)
        {
            var tbl = SetupBadNamesTable(dbType);
            var col = tbl.DiscoverColumn(BadColumnName);

            Assert.AreEqual(0,tbl.GetRowCount());

            tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
            tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
            tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,DBNull.Value } });
            
            Assert.AreEqual(3,tbl.GetRowCount());

            var topx = col.GetTopXSql(5,noNulls);

            var svr = tbl.Database.Server;
            using(var con = svr.GetConnection())
            {
                con.Open();
                var cmd = svr.GetCommand(topx,con);
                var r= cmd.ExecuteReader();

                Assert.IsTrue(r.Read());
                Assert.IsTrue(r.Read());

                Assert.AreEqual(!noNulls,r.Read());
                
                Assert.IsFalse(r.Read());
            }

            tbl.Drop();

        }
        
        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void BadNames_DropColumn(DatabaseType dbType)
        {
            var tbl = SetupBadNamesTable(dbType);

            Assert.AreEqual(2,tbl.DiscoverColumns().Length);

            var col = tbl.DiscoverColumn(BadColumnName);
            
            tbl.DropColumn(col);
            
            Assert.AreEqual(1,tbl.DiscoverColumns().Length);

            tbl.Drop();
        }

        /////////// Table tests ///////////////////

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void BadNames_TopXTable(DatabaseType dbType)
        {
            var tbl = SetupBadNamesTable(dbType);
            var col = tbl.DiscoverColumn(BadColumnName);

            Assert.AreEqual(0,tbl.GetRowCount());

            tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
            tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
            tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,DBNull.Value } });
            
            string topx = tbl.GetTopXSql(2);

            var svr = tbl.Database.Server;
            using(var con = svr.GetConnection())
            {
                con.Open();
                var cmd = svr.GetCommand(topx,con);
                var r= cmd.ExecuteReader();

                Assert.IsTrue(r.Read());
                Assert.IsTrue(r.Read());
                Assert.IsFalse(r.Read());
            }

            tbl.Drop();
        }
        
        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void BadNames_DiscoverRelationships(DatabaseType dbType)
        {
           var db = GetTestDatabase(dbType);

            
            var tbl1 = db.CreateTable(BadTableName,new[]
            {
                new DatabaseColumnRequest(BadColumnName,new DatabaseTypeRequest(typeof(string),100)){IsPrimaryKey = true }, 
                new DatabaseColumnRequest("Frrrrr ##' ank",new DatabaseTypeRequest(typeof(int))) 
            });

            DiscoveredColumn pk = tbl1.DiscoverColumns().Single(c=>c.IsPrimaryKey);
            DatabaseColumnRequest fk;

            var tbl2 = db.CreateTable(new CreateTableArgs(db,BadTableName+"2",null)
            {
                ExplicitColumnDefinitions = new []{fk = new DatabaseColumnRequest(BadColumnName+"2",new DatabaseTypeRequest(typeof(string),100)) }, 
                ForeignKeyPairs = new Dictionary<DatabaseColumnRequest, DiscoveredColumn> {{fk, pk} }
            });

            var r = tbl1.DiscoverRelationships().Single();

            Assert.AreEqual(tbl1,r.PrimaryKeyTable);
            Assert.AreEqual(tbl2,r.ForeignKeyTable);

            Assert.AreEqual(pk, r.Keys.Single().Key);
            Assert.AreEqual(tbl2.DiscoverColumn(BadColumnName +"2"), r.Keys.Single().Value);
                
            tbl2.Drop();
            tbl1.Drop();
        }

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void BadNames_BulkInsert(DatabaseType dbType)
        {
            var tbl = SetupBadNamesTable(dbType);
            
            var dt = new DataTable();
            dt.Columns.Add(BadColumnName);
            dt.Columns.Add(BadColumnName2);
            
            dt.Rows.Add (new object[]{ "fff",5});

            using(var insert = tbl.BeginBulkInsert())
            {
                insert.Upload(dt);
            }
            
            dt.Dispose();

            Assert.AreEqual(1,dt.Rows.Count);

            tbl.Drop();
        }
    }
}
