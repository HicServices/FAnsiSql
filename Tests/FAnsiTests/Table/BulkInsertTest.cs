using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FAnsi;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Exceptions;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table
{
    internal class BulkInsertTest : DatabaseTests
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
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
                });

            //There are no rows in the table yet
            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using (IBulkCopy bulk = tbl.BeginBulkInsert())
            {
                bulk.Timeout = 30;
                bulk.Upload(dt);

                Assert.AreEqual(2, tbl.GetRowCount());

                dt.Rows.Clear();
                dt.Rows.Add("Frank", 100);

                bulk.Upload(dt);

                Assert.AreEqual(3, tbl.GetRowCount());
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_ColumnOrdinals(DatabaseType type)
        {
            DiscoveredDatabase db = GetTestDatabase(type);

            DiscoveredTable tbl = db.CreateTable("MyBulkInsertTest",
                new[]
                {
                    new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
                });

            //There are no rows in the table yet
            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("Age");
            dt.Columns.Add("Name");
            dt.Rows.Add( "50","David");
            dt.Rows.Add("60","Jamie");

            Assert.AreEqual("Age",dt.Columns[0].ColumnName);
            Assert.AreEqual(typeof(string),dt.Columns[0].DataType);

            using (IBulkCopy bulk = tbl.BeginBulkInsert())
            {
                bulk.Timeout = 30;
                bulk.Upload(dt);

                Assert.AreEqual(2, tbl.GetRowCount());
            }

            //columns should not be reordered
            Assert.AreEqual("Age",dt.Columns[0].ColumnName);
            Assert.AreEqual(typeof(int),dt.Columns[0].DataType); //but the data type was changed by HardTyping it
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
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
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
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
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
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
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
                    col.DataType.Resize(100, transaction.ManagedTransaction);

                    bulk.Upload(dt);

                    //inside transaction the count is 3
                    Assert.AreEqual(3, tbl.GetRowCount(transaction.ManagedTransaction));
                }

                transaction.ManagedTransaction.CommitAndCloseConnection();
            }

            //We abandoned transaction so final rowcount should be 0
            Assert.AreEqual(3, tbl.GetRowCount());
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void BulkInsert_MixedCase(DatabaseType type)
        {
            var db = GetTestDatabase(type, true);

            var tbl = db.CreateTable("Test", new[]
            {
                new DatabaseColumnRequest("bob", new DatabaseTypeRequest(typeof (string), 100)),
                new DatabaseColumnRequest("Frank", new DatabaseTypeRequest(typeof (string), 100))
            });

            DataTable dt = new DataTable();
                //note that the column order here is reversed i.e. the DataTable column order doesn't match the database (intended)
            dt.Columns.Add("BoB");
            dt.Columns.Add("fRAnk");
            dt.Rows.Add("no", "yes");
            dt.Rows.Add("no", "no");

            using (var blk = tbl.BeginBulkInsert())
                blk.Upload(dt);

            var result = tbl.GetDataTable();
            Assert.AreEqual(2, result.Rows.Count); //2 rows inserted
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void UnmatchedColumnsBulkInsertTest_UsesDefaultValues_Passes(DatabaseType type)
        {
            var db = GetTestDatabase(type, true);

            var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
            {
                new DatabaseColumnRequest("bob", new DatabaseTypeRequest(typeof (string), 100))
                {
                    IsPrimaryKey = true,
                    AllowNulls = false
                },
                new DatabaseColumnRequest("frank", new DatabaseTypeRequest(typeof (DateTime), 100))
                {
                    Default = MandatoryScalarFunctions.GetTodaysDate
                },
                new DatabaseColumnRequest("peter", new DatabaseTypeRequest(typeof (string), 100)) {AllowNulls = false},
            });

            DataTable dt = new DataTable();
                //note that the column order here is reversed i.e. the DataTable column order doesn't match the database (intended)
            dt.Columns.Add("peter");
            dt.Columns.Add("bob");
            dt.Rows.Add("no", "yes");

            using (var blk = tbl.BeginBulkInsert())
            {
                blk.Upload(dt);
            }

            var result = tbl.GetDataTable();
            Assert.AreEqual(3, result.Columns.Count);
            Assert.AreEqual("yes", result.Rows[0]["bob"]);
            Assert.NotNull(result.Rows[0]["frank"]);
            Assert.GreaterOrEqual(result.Rows[0]["frank"].ToString().Length, 5); //should be a date
            Assert.AreEqual("no", result.Rows[0]["peter"]);

            tbl.Drop();
        }

        /// <summary>
        /// Tests creating large batches and inserting them into the database.  This test is expected to take a while.  Since at the end of the test we have a lot of data
        /// in the database we take the opportunity to test timeout/command cancellation.  The cancellation window is 100ms so if a DBMS can make the primary key within that window
        /// then maybe this test will be inconsistent?
        /// </summary>
        /// <param name="type"></param>
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void UnmatchedColumnsBulkInsertTest_UsesDefaultValues_TwoLargeBatches_Passes(DatabaseType type)
        {
            const int numberOfRowsPerBatch = 100010;

            var db = GetTestDatabase(type, true);

            var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
            {
                new DatabaseColumnRequest("bob", new DatabaseTypeRequest(typeof (string), 100))
                {
                    AllowNulls = false
                },
                new DatabaseColumnRequest("frank", new DatabaseTypeRequest(typeof (DateTime), 100))
                {
                    Default = MandatoryScalarFunctions.GetTodaysDate
                },
                new DatabaseColumnRequest("peter", new DatabaseTypeRequest(typeof (string), 100)) {AllowNulls = false},

                new DatabaseColumnRequest("Column0", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column1", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column2", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column3", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column4", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column5", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column6", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column7", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column8", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column9", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},

                new DatabaseColumnRequest("Column10", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column11", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column12", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column13", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column14", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column15", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column16", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column17", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column18", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column19", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},

                new DatabaseColumnRequest("Column20", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column21", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column22", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column23", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column24", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column25", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column26", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column27", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column28", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
                new DatabaseColumnRequest("Column29", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},

            });

            DataTable dt = new DataTable();
                //note that the column order here is reversed i.e. the DataTable column order doesn't match the database (intended)
            dt.Columns.Add("peter");
            dt.Columns.Add("bob");

            for (int i = 0; i < 30; i++)
            {
                dt.Columns.Add("Column" + i);
            }



            for (int i = 0; i < numberOfRowsPerBatch; i++)
                dt.Rows.Add("no", Guid.NewGuid().ToString(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            using (var blk = tbl.BeginBulkInsert())
            {
                Assert.AreEqual(numberOfRowsPerBatch, blk.Upload(dt)); //affected rows should match batch size
            }
            sw.Stop();
            Console.WriteLine("Time taken:" + sw.ElapsedMilliseconds + "ms");

            dt.Rows.Clear();

            for (int i = 0; i < numberOfRowsPerBatch; i++)
                dt.Rows.Add("no", Guid.NewGuid().ToString(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);

            sw.Restart();

            using (var blk = tbl.BeginBulkInsert())
            {
                Assert.AreEqual(numberOfRowsPerBatch, blk.Upload(dt));
            }
            sw.Stop();
            Console.WriteLine("Time taken:" + sw.ElapsedMilliseconds + "ms");
            
            var result = tbl.GetDataTable();
            Assert.AreEqual(33, result.Columns.Count);
            Assert.AreEqual(numberOfRowsPerBatch*2, result.Rows.Count);
            Assert.NotNull(result.Rows[0]["bob"]);
            Assert.NotNull(result.Rows[0]["frank"]);
            Assert.GreaterOrEqual(result.Rows[0]["frank"].ToString().Length, 5); //should be a date
            Assert.AreEqual("no", result.Rows[0]["peter"]);
            
            //while we have a ton of data in there lets test some cancellation operations
            
            //no primary key
            var bobCol = tbl.DiscoverColumn("bob");
            Assert.IsFalse(tbl.DiscoverColumns().Any(c=>c.IsPrimaryKey));


            CancellationTokenSource cts;
            using (var con = tbl.Database.Server.BeginNewTransactedConnection())
            {
                //give it 100 ms delay (simulates user cancelling not DbCommand.Timeout expiring)
                cts = new CancellationTokenSource(100);

                //creation should have been cancelled at the database level
                var ex = Assert.Throws<AlterFailedException>(()=>tbl.CreatePrimaryKey(con.ManagedTransaction,cts.Token,50000,bobCol));
                
                //MySql seems to be throwing null reference inside ExecuteNonQueryAsync.  No idea why but it is still cancelled
                if(type != DatabaseType.MySql)
                    StringAssert.Contains("cancel",ex.InnerException.Message);
                else
                    Console.WriteLine("MySql error was:" + ex.InnerException);
            }

            //Now lets test cancelling GetDataTable
            using (var con = tbl.Database.Server.BeginNewTransactedConnection())
            {
                //give it 100 ms delay (simulates user cancelling not DbCommand.Timeout expiring)
                cts = new CancellationTokenSource(300);

                //GetDataTable should have been cancelled at the database level
                var ex = Assert.Throws<OperationCanceledException>(()=>tbl.GetDataTable(new DatabaseOperationArgs(con.ManagedTransaction,cts.Token,50000)));
                tbl.GetDataTable(new DatabaseOperationArgs(con.ManagedTransaction,default(CancellationToken),50000));
            }

            
            //and there should not be any primary keys
            Assert.IsFalse(tbl.DiscoverColumns().Any(c=>c.IsPrimaryKey));

            //now give it a bit longer to create it
            cts = new CancellationTokenSource(50000000);
            tbl.CreatePrimaryKey(null, cts.Token, 50000, bobCol);

            bobCol = tbl.DiscoverColumn("bob");
            Assert.IsTrue(bobCol.IsPrimaryKey);
            
            tbl.Drop();
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void NullPrimaryKey_ThrowsException(DatabaseType type)
        {
            var db = GetTestDatabase(type, true);

            var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
            {
                new DatabaseColumnRequest("bob", new DatabaseTypeRequest(typeof (string), 100))
                {
                    IsPrimaryKey = true,
                    AllowNulls = false
                }
            });

            DataTable dt = new DataTable();
            dt.Columns.Add("bob");
            dt.Rows.Add(DBNull.Value);

            using (var blk = tbl.BeginBulkInsert())
            {
                Assert.Throws(Is.InstanceOf<Exception>(), () => blk.Upload(dt));
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void AutoIncrementPrimaryKey_Passes(DatabaseType type)
        {
            var db = GetTestDatabase(type, true);

            var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
            {
                new DatabaseColumnRequest("bob", new DatabaseTypeRequest(typeof (int), 100))
                {
                    IsPrimaryKey = true,
                    AllowNulls = false,
                    IsAutoIncrement = true
                },
                new DatabaseColumnRequest("frank", new DatabaseTypeRequest(typeof (string), 100))
                {
                    IsPrimaryKey = true,
                    AllowNulls = false
                }
            });

            DataTable dt = new DataTable();
            dt.Columns.Add("frank");
            dt.Rows.Add("fish");
            dt.Rows.Add("fish");
            dt.Rows.Add("tank");

            using (var blk = tbl.BeginBulkInsert())
            {
                Assert.AreEqual(3, blk.Upload(dt));
            }


            var result = tbl.GetDataTable();

            Assert.AreEqual(2, result.Columns.Count);
            Assert.AreEqual(3, result.Rows.Count);
            Assert.AreEqual(1, result.Rows.Cast<DataRow>().Count(r => Convert.ToInt32(r["bob"]) == 1));
            Assert.AreEqual(1, result.Rows.Cast<DataRow>().Count(r => Convert.ToInt32(r["bob"]) == 2));
            Assert.AreEqual(1, result.Rows.Cast<DataRow>().Count(r => Convert.ToInt32(r["bob"]) == 3));

        }


        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_ScientificNotation(DatabaseType type)
        {
            var db = GetTestDatabase(type, true);

            var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
            {
                new DatabaseColumnRequest("num", new DatabaseTypeRequest(typeof (decimal), null,new DecimalSize(1,10)))
                {
                    AllowNulls = false,
                }
            });

            DataTable dt = new DataTable();
            dt.Columns.Add("num");
            dt.Rows.Add("-4.10235746055587E-05"); //-0.0000410235746055587  <- this is what the number is
                                                              //-0.0000410235           <- this is what goes into db since we only asked for 10 digits after decimal place
            using (var blk = tbl.BeginBulkInsert())
            {
                Assert.AreEqual(1, blk.Upload(dt));
            }

            tbl.Insert(new Dictionary<string, object>() {{"num", "-4.10235746055587E-05"}});

            //the numbers read from the database should be pretty much exactly -0.0000410235 but Decimals are always a pain so...
            var result = tbl.GetDataTable();

            //right number of rows/columns?
            Assert.AreEqual(1, result.Columns.Count);
            Assert.AreEqual(2, result.Rows.Count);

            //get cell values rounded to 9 decimal places
            var c1 = Math.Round((decimal) result.Rows[0][0], 9);
            var c2 = Math.Round((decimal) result.Rows[1][0], 9);

            //make sure they are basically what we are expecting (at the 9 decimal place point)
            if (Math.Abs(-0.0000410235 - (double) c1) < 0.000000001)
                Assert.Pass();
            else 
                Assert.Fail();

            if(Math.Abs(-0.0000410235 - (double) c2) < 0.000000001)
                Assert.Pass();
            else
                Assert.Fail();

        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MySql)]
        public void TestBulkInsert_Unicode(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);

            DataTable dt = new DataTable();
            dt.Columns.Add("Yay");
            dt.Rows.Add("乗 12345");

            var table = db.CreateTable("GoGo", dt);

            DataTable dt2 = new DataTable();
            dt2.Columns.Add("yay");
            dt2.Rows.Add("你好");

            using (var insert = table.BeginBulkInsert())
                insert.Upload(dt2);
            
            table.Insert(new Dictionary<string, object>() {{"Yay", "مرحبا"}});
            
            //now check that it all worked!

            var dtResult = table.GetDataTable();
            Assert.AreEqual(3,dtResult.Rows.Count);
            
            //value fetched from database should match the one inserted
            Assert.Contains("乗 12345",dtResult.Rows.Cast<DataRow>().Select(r=>r[0]).ToArray());
            Assert.Contains("你好",dtResult.Rows.Cast<DataRow>().Select(r=>r[0]).ToArray());
            Assert.Contains("مرحبا",dtResult.Rows.Cast<DataRow>().Select(r=>r[0]).ToArray());
            table.Drop();
        }   

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_SchemaTooNarrow_StringError(DatabaseType type)
        {
            DiscoveredDatabase db = GetTestDatabase(type);

            DiscoveredTable tbl = db.CreateTable("MyBulkInsertTest",
                new[]
                {
                    new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof (int))){IsAutoIncrement = true, IsPrimaryKey = true},
                    new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
                });

            //There are no rows in the table yet
            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("age");
            dt.Columns.Add("name");

            dt.Rows.Add(60,"Jamie");
            dt.Rows.Add(30,"Frank");
            dt.Rows.Add(11,"Toad");
            dt.Rows.Add(50, new string('A', 11));    
            dt.Rows.Add(100,"King");
            dt.Rows.Add(10,"Frog");        

            using (IBulkCopy bulk = tbl.BeginBulkInsert())
            {
                bulk.Timeout = 30;
                
                Exception ex = null;
                try 
                {
                    bulk.Upload(dt);
                }
                catch(Exception e)
                {
                    ex = e;
                }

                Assert.IsNotNull(ex,"Expected upload to fail because value on row 2 is too long");

                switch (type)
                {
                    case DatabaseType.MicrosoftSQLServer:
                        StringAssert.Contains("BulkInsert failed on data row 4 the complaint was about source column <<name>> which had value <<AAAAAAAAAAA>> destination data type was <<varchar(10)>>",ex.Message);
                        break;
                    case DatabaseType.MySql:
                        Assert.AreEqual("Data too long for column 'Name' at row 4",ex.Message);
                        break;
                    case DatabaseType.Oracle:
                        StringAssert.Contains("NAME",ex.Message);
                        StringAssert.Contains("maximum: 10",ex.Message);
                        StringAssert.Contains("actual: 11",ex.Message);

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                }
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void TestBulkInsert_SchemaTooNarrow_DecimalError(DatabaseType type)
        {
            DiscoveredDatabase db = GetTestDatabase(type);
            
            DiscoveredTable tbl = db.CreateTable("MyBulkInsertTest",
                new[]
                {
                    new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof (int))){IsAutoIncrement = true, IsPrimaryKey = true},
                    new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                    new DatabaseColumnRequest("Score", new DatabaseTypeRequest(typeof (decimal), null,new DecimalSize(2,1))),
                    new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
                });

            //There are no rows in the table yet
            Assert.AreEqual(0, tbl.GetRowCount());

            var dt = new DataTable();
            dt.Columns.Add("age");
            dt.Columns.Add("name");
            dt.Columns.Add("score");

            dt.Rows.Add(60,"Jamie",1.2);
            dt.Rows.Add(30,"Frank",1.3);
            dt.Rows.Add(11,"Toad",111111111.11); //bad data 
            dt.Rows.Add(100,"King");
            dt.Rows.Add(10,"Frog");        

            using (IBulkCopy bulk = tbl.BeginBulkInsert())
            {
                bulk.Timeout = 30;
                
                Exception ex = null;
                try 
                {
                    bulk.Upload(dt);
                }
                catch(Exception e)
                {
                    ex = e;
                }

                Assert.IsNotNull(ex,"Expected upload to fail because value on row 2 is too long");

                switch (type)
                {
                    case DatabaseType.MicrosoftSQLServer:
                        StringAssert.Contains("Failed to load data row 3 the following values were rejected by the database",ex.Message);
                        StringAssert.Contains("Parameter value '111111111.1' is out of range",ex.Message);
                        break;
                    case DatabaseType.MySql:
                        Assert.AreEqual("Out of range value for column 'Score' at row 3",ex.Message);
                        break;
                    case DatabaseType.Oracle:
                        StringAssert.Contains("value larger than specified precision allowed for this column",ex.Message);

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                }
            }
        }
    }
}
