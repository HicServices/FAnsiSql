using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System;
using System.Collections.Generic;
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

        private DiscoveredTable SetupBadNamesTable(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);

            return db.CreateTable(BadTableName,new[]
            {
                new DatabaseColumnRequest(BadColumnName,new DatabaseTypeRequest(typeof(string),100)), 
                new DatabaseColumnRequest("Frrrrr ##' ank",new DatabaseTypeRequest(typeof(int))) 
            });

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
        }

    }
}
