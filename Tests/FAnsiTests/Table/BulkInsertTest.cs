using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using FansiTests;
using NUnit.Framework;

namespace FAnsiTests.Table
{
    class BulkInsertTest:DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_Basic(DatabaseType type)
        {
            DiscoveredDatabase db = GetTestDatabase(type);

            DiscoveredTable tbl = db.CreateTable("MyBulkInsertTest",
                new[]
                {
                    new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
                });

            //There are no rows in the table yet
            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie",60);

            using (IBulkCopy bulk = tbl.BeginBulkInsert())
            {
                bulk.Timeout = 30;
                bulk.Upload(dt);

                Assert.AreEqual(2,tbl.GetRowCount());

                dt.Rows.Clear();
                dt.Rows.Add("Frank", 100);
                
                bulk.Upload(dt);

                Assert.AreEqual(3, tbl.GetRowCount());
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_Transaction(DatabaseType type)
        {
            DiscoveredDatabase db = GetTestDatabase(type);

            DiscoveredTable tbl = db.CreateTable("MyBulkInsertTest",
                new[]
                {
                    new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
                });


            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using (var transaction = tbl.Database.Server.BeginNewTransactedConnection())
            {
                using (IBulkCopy bulk = tbl.BeginBulkInsert(transaction.ManagedTransaction))
                {
                    bulk.Timeout = 30;
                    bulk.Upload(dt);
                    
                    //inside transaction the count is 2
                    Assert.AreEqual(2, tbl.GetRowCount(transaction.ManagedTransaction));

                    dt.Rows.Clear();
                    dt.Rows.Add("Frank", 100);

                    bulk.Upload(dt);

                    //inside transaction the count is 3
                    Assert.AreEqual(3, tbl.GetRowCount(transaction.ManagedTransaction));
                }

                transaction.ManagedTransaction.CommitAndCloseConnection();
            }

            //Transaction was committed final row count should be 3
            Assert.AreEqual(3, tbl.GetRowCount());
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_AbandonTransaction(DatabaseType type)
        {
            var db = GetTestDatabase(type);

            var tbl = db.CreateTable("MyBulkInsertTest",
                new[]
                {
                    new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
                });


            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using(var transaction = tbl.Database.Server.BeginNewTransactedConnection())
            {
                using (var bulk = tbl.BeginBulkInsert(transaction.ManagedTransaction))
                {
                    bulk.Timeout = 30;
                    bulk.Upload(dt);

                    //inside transaction the count is 2
                    Assert.AreEqual(2, tbl.GetRowCount(transaction.ManagedTransaction));

                    dt.Rows.Clear();
                    dt.Rows.Add("Frank", 100);

                    bulk.Upload(dt);

                    //inside transaction the count is 3
                    Assert.AreEqual(3, tbl.GetRowCount(transaction.ManagedTransaction));
                }

                transaction.ManagedTransaction.AbandonAndCloseConnection();
            }

            //We abandoned transaction so final rowcount should be 0
            Assert.AreEqual(0, tbl.GetRowCount());
        }


        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_AlterColumn_MidTransaction(DatabaseType type)
        {
            var db = GetTestDatabase(type);

            var tbl = db.CreateTable("MyBulkInsertTest",
                new[]
                {
                    new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
                });
            
            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using (var transaction = tbl.Database.Server.BeginNewTransactedConnection())
            {
                using (var bulk = tbl.BeginBulkInsert(transaction.ManagedTransaction))
                {
                    bulk.Timeout = 30;
                    bulk.Upload(dt);

                    //inside transaction the count is 2
                    Assert.AreEqual(2, tbl.GetRowCount(transaction.ManagedTransaction));
                    
                    //New row is too long for the data type
                    dt.Rows.Clear();
                    dt.Rows.Add("Frankyyyyyyyyyyyyyyyyyyyyyy", 100);

                    //So alter the data type to handle up to string lengths of 100
                    //Find the column
                    DiscoveredColumn col = tbl.DiscoverColumn("Name", transaction.ManagedTransaction);

                    //Make it bigger
                    col.DataType.Resize(100,transaction.ManagedTransaction);
                    
                    bulk.Upload(dt);

                    //inside transaction the count is 3
                    Assert.AreEqual(3, tbl.GetRowCount(transaction.ManagedTransaction));
                }

                transaction.ManagedTransaction.CommitAndCloseConnection();
            }

            //We abandoned transaction so final rowcount should be 0
            Assert.AreEqual(3, tbl.GetRowCount());
        }
    }
}
