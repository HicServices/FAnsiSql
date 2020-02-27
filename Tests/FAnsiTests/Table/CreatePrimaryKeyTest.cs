using System;
using System.Data;
using System.Data.Common;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Exceptions;
using NUnit.Framework;

namespace FAnsiTests.Table
{
    class CreatePrimaryKeyTest: DatabaseTests
    {
        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void TestBasicCase_KeysCreated(DatabaseType databaseType)
        {
            DiscoveredTable tbl;
            using (var dt = new DataTable("Fish"))
            {
                dt.Columns.Add("A");
                dt.Columns.Add("B");
                dt.Columns.Add("C");

                dt.Rows.Add("a1", null, null);
                dt.Rows.Add("a2", null, null);
                dt.Rows.Add("a3", null, null);

                var db = GetTestDatabase(databaseType);

                tbl = db.CreateTable("Fish", dt);
            }
            
            var col = tbl.DiscoverColumn("A");

            Assert.IsTrue(col.AllowNulls);
            Assert.IsFalse(col.IsPrimaryKey);

            tbl.CreatePrimaryKey(col);
            
            col = tbl.DiscoverColumn("A");

            Assert.IsFalse(col.AllowNulls);
            Assert.IsTrue(col.IsPrimaryKey);
        }

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void TestBasicCase_FailHalfWay_SchemaUnchanged(DatabaseType databaseType)
        {
            DiscoveredTable tbl;
            using (var dt = new DataTable("Fish"))
            {
                dt.Columns.Add("A");
                dt.Columns.Add("B");
                dt.Columns.Add("C");

                dt.Rows.Add("a1", "b1", null);
                dt.Rows.Add("a2", null, null);
                dt.Rows.Add("a3", "b2", null);

                var db = GetTestDatabase(databaseType);

                tbl = db.CreateTable("Fish", dt);
            }
            
            var colA = tbl.DiscoverColumn("A");
            var colB = tbl.DiscoverColumn("B");

            //Pre state
            Assert.IsTrue(colA.AllowNulls);
            Assert.IsFalse(colA.IsPrimaryKey);
            Assert.IsTrue(colB.AllowNulls);
            Assert.IsFalse(colB.IsPrimaryKey);

            var ex = Assert.Throws<AlterFailedException>(()=>tbl.CreatePrimaryKey(new []{colA,colB}));
            Assert.IsTrue(ex.Message.Contains("Failed to create primary key on table"));
            Assert.IsNotInstanceOf(typeof(AggregateException), ex.InnerException);
            Assert.IsInstanceOf<DbException>(ex.InnerException);

            colA = tbl.DiscoverColumn("A");
            colB = tbl.DiscoverColumn("B");

            //Post state should exactly match
            Assert.IsFalse(colA.IsPrimaryKey);
            Assert.IsFalse(colB.IsPrimaryKey);
        }
    }
}
