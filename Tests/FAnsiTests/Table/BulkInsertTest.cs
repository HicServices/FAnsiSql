using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Exceptions;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using TypeGuesser;

namespace FAnsiTests.Table;

internal class BulkInsertTest : DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_Basic(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });

        //There are no rows in the table yet
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using var dt = new DataTable();
        dt.Columns.Add("Name");
        dt.Columns.Add("Age");
        dt.Rows.Add("Dave", 50);
        dt.Rows.Add("Jamie", 60);

        using var bulk = tbl.BeginBulkInsert();
        bulk.Timeout = 30;
        bulk.Upload(dt);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(2));

        dt.Rows.Clear();
        dt.Rows.Add("Frank", 100);

        bulk.Upload(dt);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void TestBulkInsert_SpacedOutNames(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Na me", new DatabaseTypeRequest(typeof(string), 10)),
                new DatabaseColumnRequest("A ge", new DatabaseTypeRequest(typeof(int)))
            });

        //There are no rows in the table yet
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Na me");
            dt.Columns.Add("A ge");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using var bulk = tbl.BeginBulkInsert();
            bulk.Timeout = 30;
            bulk.Upload(dt);

            Assert.That(tbl.GetRowCount(), Is.EqualTo(2));

            dt.Rows.Clear();
            dt.Rows.Add("Frank", 100);

            bulk.Upload(dt);

            Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
        }

        tbl.Insert(new Dictionary<string, object>
        {
            {"Na me", "George"},
            {"A ge", "300"}
        });

        Assert.That(tbl.GetRowCount(), Is.EqualTo(4));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_ColumnOrdinals(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });

        //There are no rows in the table yet
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using var dt = new DataTable();
        dt.Columns.Add("Age");
        dt.Columns.Add("Name");
        dt.Rows.Add( "50","David");
        dt.Rows.Add("60","Jamie");

        Assert.Multiple(() =>
        {
            Assert.That(dt.Columns[0].ColumnName, Is.EqualTo("Age"));
            Assert.That(dt.Columns[0].DataType, Is.EqualTo(typeof(string)));
        });

        using (var bulk = tbl.BeginBulkInsert())
        {
            bulk.Timeout = 30;
            bulk.Upload(dt);

            Assert.That(tbl.GetRowCount(), Is.EqualTo(2));
        }

        Assert.Multiple(() =>
        {
            //columns should not be reordered
            Assert.That(dt.Columns[0].ColumnName, Is.EqualTo("Age"));
            Assert.That(dt.Columns[0].DataType, Is.EqualTo(typeof(int))); //but the data type was changed by HardTyping it
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_Transaction(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });


        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using var transaction = tbl.Database.Server.BeginNewTransactedConnection();
            using (var bulk = tbl.BeginBulkInsert(transaction.ManagedTransaction))
            {
                bulk.Timeout = 30;
                bulk.Upload(dt);

                //inside transaction the count is 2
                Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(2));

                dt.Rows.Clear();
                dt.Rows.Add("Frank", 100);

                bulk.Upload(dt);

                //inside transaction the count is 3
                Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(3));
            }

            transaction.ManagedTransaction.CommitAndCloseConnection();
        }

        //Transaction was committed final row count should be 3
        Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_AbandonTransaction(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });


        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using var transaction = tbl.Database.Server.BeginNewTransactedConnection();
            using (var bulk = tbl.BeginBulkInsert(transaction.ManagedTransaction))
            {
                bulk.Timeout = 30;
                bulk.Upload(dt);

                //inside transaction the count is 2
                Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(2));

                dt.Rows.Clear();
                dt.Rows.Add("Frank", 100);

                bulk.Upload(dt);

                //inside transaction the count is 3
                Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(3));
            }

            transaction.ManagedTransaction.AbandonAndCloseConnection();
        }

        //We abandoned transaction so final rowcount should be 0
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_AlterColumn_MidTransaction(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });

        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Name");
            dt.Columns.Add("Age");
            dt.Rows.Add("Dave", 50);
            dt.Rows.Add("Jamie", 60);

            using var transaction = tbl.Database.Server.BeginNewTransactedConnection();
            using (var bulk = tbl.BeginBulkInsert(transaction.ManagedTransaction))
            {
                bulk.Timeout = 30;
                bulk.Upload(dt);

                //inside transaction the count is 2
                Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(2));

                //New row is too long for the data type
                dt.Rows.Clear();
                dt.Rows.Add("Frankyyyyyyyyyyyyyyyyyyyyyy", 100);

                //So alter the data type to handle up to string lengths of 100
                //Find the column
                var col = tbl.DiscoverColumn("Name", transaction.ManagedTransaction);

                //Make it bigger
                col.DataType.Resize(100, transaction.ManagedTransaction);

                bulk.Upload(dt);

                //inside transaction the count is 3
                Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(3));
            }

            transaction.ManagedTransaction.CommitAndCloseConnection();
        }

        //We abandoned transaction so final rowcount should be 0
        Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BulkInsert_MixedCase(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("Test", new[]
        {
            new DatabaseColumnRequest("bob", new DatabaseTypeRequest(typeof (string), 100)),
            new DatabaseColumnRequest("Frank", new DatabaseTypeRequest(typeof (string), 100))
        });

        using (var dt = new DataTable())
        {
            //note that the column order here is reversed i.e. the DataTable column order doesn't match the database (intended)
            dt.Columns.Add("BoB");
            dt.Columns.Add("fRAnk");
            dt.Rows.Add("no", "yes");
            dt.Rows.Add("no", "no");

            using var blk = tbl.BeginBulkInsert();
            blk.Upload(dt);
        }

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(2)); //2 rows inserted
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void UnmatchedColumnsBulkInsertTest_UsesDefaultValues_Passes(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
        {
            new("bob", new DatabaseTypeRequest(typeof (string), 100))
            {
                IsPrimaryKey = true,
                AllowNulls = false
            },
            new("frank", new DatabaseTypeRequest(typeof (DateTime), 100))
            {
                Default = MandatoryScalarFunctions.GetTodaysDate
            },
            new("peter", new DatabaseTypeRequest(typeof (string), 100)) {AllowNulls = false}
        });

        using (var dt = new DataTable())
        {
            //note that the column order here is reversed i.e. the DataTable column order doesn't match the database (intended)
            dt.Columns.Add("peter");
            dt.Columns.Add("bob");
            dt.Rows.Add("no", "yes");

            using var blk = tbl.BeginBulkInsert();
            blk.Upload(dt);
        }


        var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(result.Columns, Has.Count.EqualTo(3));
            Assert.That(result.Rows[0]["bob"], Is.EqualTo("yes"));
            Assert.That(result.Rows[0]["frank"], Is.Not.Null);
        });
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["frank"].ToString()?.Length, Is.GreaterThanOrEqualTo(5)); //should be a date
            Assert.That(result.Rows[0]["peter"], Is.EqualTo("no"));
        });

        tbl.Drop();
    }

    /// <summary>
    /// Tests creating large batches and inserting them into the database.  This test is expected to take a while.  Since at the end of the test we have a lot of data
    /// in the database we take the opportunity to test timeout/command cancellation.  The cancellation window is 100ms so if a DBMS can make the primary key within that window
    /// then maybe this test will be inconsistent?
    /// </summary>
    /// <param name="type"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void UnmatchedColumnsBulkInsertTest_UsesDefaultValues_TwoLargeBatches_Passes(DatabaseType type)
    {
        const int numberOfRowsPerBatch = 100010;

        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
        {
            new("bob", new DatabaseTypeRequest(typeof (string), 100))
            {
                AllowNulls = false
            },
            new("frank", new DatabaseTypeRequest(typeof (DateTime), 100))
            {
                Default = MandatoryScalarFunctions.GetTodaysDate
            },
            new("peter", new DatabaseTypeRequest(typeof (string), 100)) {AllowNulls = false},

            new("Column0", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column1", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column2", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column3", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column4", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column5", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column6", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column7", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column8", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column9", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},

            new("Column10", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column11", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column12", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column13", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column14", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column15", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column16", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column17", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column18", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column19", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},

            new("Column20", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column21", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column22", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column23", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column24", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column25", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column26", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column27", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column28", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false},
            new("Column29", new DatabaseTypeRequest(typeof (int))) {AllowNulls = false}

        });

        using (var dt = new DataTable())
        {
            //note that the column order here is reversed i.e. the DataTable column order doesn't match the database (intended)
            dt.Columns.Add("peter");
            dt.Columns.Add("bob");

            for (var i = 0; i < 30; i++)
            {
                dt.Columns.Add($"Column{i}");
            }



            for (var i = 0; i < numberOfRowsPerBatch; i++)
                dt.Rows.Add("no", Guid.NewGuid().ToString(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);

            var sw = new Stopwatch();
            sw.Start();

            using (var blk = tbl.BeginBulkInsert())
            {
                Assert.That(blk.Upload(dt), Is.EqualTo(numberOfRowsPerBatch)); //affected rows should match batch size
            }
            sw.Stop();
            TestContext.WriteLine($"Time taken:{sw.ElapsedMilliseconds}ms");

            dt.Rows.Clear();

            for (var i = 0; i < numberOfRowsPerBatch; i++)
                dt.Rows.Add("no", Guid.NewGuid().ToString(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);

            sw.Restart();

            using (var blk = tbl.BeginBulkInsert())
            {
                Assert.That(blk.Upload(dt), Is.EqualTo(numberOfRowsPerBatch));
            }

            sw.Stop();
            TestContext.WriteLine($"Time taken:{sw.ElapsedMilliseconds}ms");
        }


        var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(result.Columns, Has.Count.EqualTo(33));
            Assert.That(result.Rows, Has.Count.EqualTo(numberOfRowsPerBatch * 2));
        });
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["bob"], Is.Not.Null);
            Assert.That(result.Rows[0]["frank"], Is.Not.Null);
        });
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["frank"].ToString()?.Length, Is.GreaterThanOrEqualTo(5)); //should be a date
            Assert.That(result.Rows[0]["peter"], Is.EqualTo("no"));
        });

        //while we have a ton of data in there let's test some cancellation operations

        //no primary key
        var bobCol = tbl.DiscoverColumn("bob");
        Assert.That(!tbl.DiscoverColumns().Any(static c=>c.IsPrimaryKey), Is.True);


        using (var con = tbl.Database.Server.BeginNewTransactedConnection())
        {
            // Create and cancel a CTS (simulates user cancelling not DbCommand.Timeout expiring) - any delay and Oracle will actually complete regardless...
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            //creation should have been cancelled at the database level
            var ex = Assert.Throws<AlterFailedException>(()=>tbl.CreatePrimaryKey(con.ManagedTransaction,cts.Token,50000,bobCol));

            //MySql seems to be throwing null reference inside ExecuteNonQueryAsync.  No idea why but it is still cancelled
            if(type != DatabaseType.MySql)
                StringAssert.Contains("cancel",ex?.InnerException?.Message);
            else
                TestContext.WriteLine($"MySql error was:{ex?.InnerException?.Message}");
        }

        //Now let's test cancelling GetDataTable
        using (var con = tbl.Database.Server.BeginNewTransactedConnection())
        {
            //give it 300 ms delay (simulates user cancelling not DbCommand.Timeout expiring)
            using var cts = new CancellationTokenSource(300);
            //GetDataTable should have been cancelled at the database level
            Assert.Throws<OperationCanceledException>(()=>tbl.GetDataTable(new DatabaseOperationArgs(con.ManagedTransaction,50000,
                cts.Token)));
            tbl.GetDataTable(new DatabaseOperationArgs(con.ManagedTransaction,50000, default));
        }


        //and there should not be any primary keys
        Assert.That(tbl.DiscoverColumns().Any(c=>c.IsPrimaryKey), Is.False);

        //now give it a bit longer to create it
        using(var cts = new CancellationTokenSource(50000000))
            tbl.CreatePrimaryKey(null, cts.Token, 50000, bobCol);

        bobCol = tbl.DiscoverColumn("bob");
        Assert.That(bobCol.IsPrimaryKey);

        tbl.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void NullPrimaryKey_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
        {
            new("bob", new DatabaseTypeRequest(typeof (string), 100))
            {
                IsPrimaryKey = true,
                AllowNulls = false
            }
        });

        using var dt = new DataTable();
        dt.Columns.Add("bob");
        dt.Rows.Add(DBNull.Value);

        using var blk = tbl.BeginBulkInsert();
        Assert.Throws(Is.InstanceOf<Exception>(), () => blk.Upload(dt));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void AutoIncrementPrimaryKey_Passes(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
        {
            new("bob", new DatabaseTypeRequest(typeof (int), 100))
            {
                IsPrimaryKey = true,
                AllowNulls = false,
                IsAutoIncrement = true
            },
            new("frank", new DatabaseTypeRequest(typeof (string), 100))
            {
                IsPrimaryKey = true,
                AllowNulls = false
            }
        });

        using (var dt = new DataTable())
        {
            dt.Columns.Add("frank");
            dt.Rows.Add("fish");
            dt.Rows.Add("fish");
            dt.Rows.Add("tank");

            using var blk = tbl.BeginBulkInsert();
            Assert.That(blk.Upload(dt), Is.EqualTo(3));
        }


        var result = tbl.GetDataTable();

        Assert.Multiple(() =>
        {
            Assert.That(result.Columns, Has.Count.EqualTo(2));
            Assert.That(result.Rows, Has.Count.EqualTo(3));
        });
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows.Cast<DataRow>().Count(r => Convert.ToInt32(r["bob"]) == 1), Is.EqualTo(1));
            Assert.That(result.Rows.Cast<DataRow>().Count(r => Convert.ToInt32(r["bob"]) == 2), Is.EqualTo(1));
            Assert.That(result.Rows.Cast<DataRow>().Count(r => Convert.ToInt32(r["bob"]) == 3), Is.EqualTo(1));
        });

    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_ScientificNotation(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("Test", new DatabaseColumnRequest[]
        {
            new("num", new DatabaseTypeRequest(typeof (decimal), null,new DecimalSize(1,10)))
            {
                AllowNulls = false
            }
        });

        using (var dt = new DataTable())
        {
            dt.Columns.Add("num");
            dt.Rows.Add("-4.10235746055587E-05"); //-0.0000410235746055587  <- this is what the number is
            //-0.0000410235           <- this is what goes into db since we only asked for 10 digits after decimal place
            using var blk = tbl.BeginBulkInsert();
            Assert.That(blk.Upload(dt), Is.EqualTo(1));
        }


        tbl.Insert(new Dictionary<string, object> {{"num", "-4.10235746055587E-05"}});

        //the numbers read from the database should be pretty much exactly -0.0000410235 but Decimals are always a pain so...
        var result = tbl.GetDataTable();

        Assert.Multiple(() =>
        {
            //right number of rows/columns?
            Assert.That(result.Columns, Has.Count.EqualTo(1));
            Assert.That(result.Rows, Has.Count.EqualTo(2));
        });

        //get cell values rounded to 9 decimal places
        var c1 = Math.Round((decimal) result.Rows[0][0], 9);
        var c2 = Math.Round((decimal) result.Rows[1][0], 9);

        //make sure they are basically what we are expecting (at the 9 decimal place point)
        if (Math.Abs(-0.0000410235 - (double)c1) >= 0.000000001)
            Assert.Fail();

        if (Math.Abs(-0.0000410235 - (double)c2) >= 0.000000001)
            Assert.Fail();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_Unicode(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable table;

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Yay");
            dt.Rows.Add("乗 12345");

            table = db.CreateTable("GoGo", dt);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("yay");
            dt2.Rows.Add("你好");

            using var insert = table.BeginBulkInsert();
            insert.Upload(dt2);
        }

        table.Insert(new Dictionary<string, object> {{"Yay", "مرحبا"}});

        //now check that it all worked!

        var dtResult = table.GetDataTable();
        Assert.That(dtResult.Rows, Has.Count.EqualTo(3));

        //value fetched from database should match the one inserted
        var values = dtResult.Rows.Cast<DataRow>().Select(static r => (string)r[0]).ToArray();
        Assert.That(values, Does.Contain("乗 12345"));
        Assert.That(values, Does.Contain("你好"));
        Assert.That(values, Does.Contain("مرحبا"));
        table.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_SchemaTooNarrow_StringError(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof (int))){IsAutoIncrement = true, IsPrimaryKey = true},
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });

        //There are no rows in the table yet
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using var dt = new DataTable();
        dt.Columns.Add("age");
        dt.Columns.Add("name");

        dt.Rows.Add(60,"Jamie");
        dt.Rows.Add(30,"Frank");
        dt.Rows.Add(11,"Toad");
        dt.Rows.Add(50, new string('A', 11));
        dt.Rows.Add(100,"King");
        dt.Rows.Add(10,"Frog");

        using var bulk = tbl.BeginBulkInsert();
        bulk.Timeout = 30;

        var ex=Assert.Catch(() => bulk.Upload(dt), "Expected upload to fail because value on row 2 is too long");

        switch (type)
        {
            case DatabaseType.MicrosoftSQLServer:
                StringAssert.Contains("BulkInsert failed on data row 4 the complaint was about source column <<name>> which had value <<AAAAAAAAAAA>> destination data type was <<varchar(10)>>",ex.Message);
                break;
            case DatabaseType.MySql:
                Assert.That(ex.Message, Is.EqualTo("Data too long for column 'Name' at row 4"));
                break;
            case DatabaseType.Oracle:
                StringAssert.Contains("NAME",ex.Message);
                StringAssert.Contains("maximum: 10",ex.Message);
                StringAssert.Contains("actual: 11",ex.Message);

                break;
            case DatabaseType.PostgreSql:
                StringAssert.Contains("value too long for type character varying(10)",ex.Message);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_ExplicitDateTimeFormats(DatabaseType type)
    {

        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyDateTestTable",
            new[]
            {
                new DatabaseColumnRequest("MyDate", new DatabaseTypeRequest(typeof (DateTime))){AllowNulls=false }
            });

        //There are no rows in the table yet
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using (var dt = new DataTable())
        {
            dt.Columns.Add("MyDate");
            dt.Rows.Add("20011230");

            using var bulk = tbl.BeginBulkInsert();
            bulk.Timeout = 30;
            bulk.DateTimeDecider.Settings.ExplicitDateFormats = new []{"yyyyMMdd" };
            bulk.Upload(dt);
        }

        var dtDown = tbl.GetDataTable();
        Assert.That(dtDown.Rows[0]["MyDate"], Is.EqualTo(new DateTime(2001,12,30)));
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_SchemaTooNarrow_DecimalError(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof (int))){IsAutoIncrement = true, IsPrimaryKey = true},
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Score", new DatabaseTypeRequest(typeof (decimal), null,new DecimalSize(2,1))),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });

        //There are no rows in the table yet
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using var dt = new DataTable();
        dt.Columns.Add("age");
        dt.Columns.Add("name");
        dt.Columns.Add("score");

        dt.Rows.Add(60,"Jamie",1.2);
        dt.Rows.Add(30,"Frank",1.3);
        dt.Rows.Add(11,"Toad",111111111.11); //bad data
        dt.Rows.Add(100,"King");
        dt.Rows.Add(10,"Frog");

        using var bulk = tbl.BeginBulkInsert();
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

        Assert.That(ex, Is.Not.Null, "Expected upload to fail because value on row 2 is too long");

        switch (type)
        {
            case DatabaseType.MicrosoftSQLServer:
                StringAssert.Contains("Failed to load data row 3 the following values were rejected by the database",ex.Message);
                StringAssert.Contains("Parameter value '111111111.1' is out of range",ex.Message);
                break;
            case DatabaseType.MySql:
                Assert.That(ex.Message, Is.EqualTo("Out of range value for column 'Score' at row 3"));
                break;
            case DatabaseType.Oracle:
                StringAssert.Contains("value larger than specified precision allowed for this column",ex.Message);

                break;
            case DatabaseType.PostgreSql:
                StringAssert.Contains("numeric field overflow",ex.Message);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestBulkInsert_BadDecimalFormat_DecimalError(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
            new[]
            {
                new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof (int))){IsAutoIncrement = true, IsPrimaryKey = true},
                new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10)),
                new DatabaseColumnRequest("Score", new DatabaseTypeRequest(typeof (decimal), null,new DecimalSize(2,1))),
                new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof (int)))
            });

        //There are no rows in the table yet
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using var dt = new DataTable();
        dt.Columns.Add("age");
        dt.Columns.Add("name");
        dt.Columns.Add("score");

        dt.Rows.Add(60,"Jamie",1.2);
        dt.Rows.Add(30,"Frank",1.3);
        dt.Rows.Add(11,"Toad","."); //bad data
        dt.Rows.Add(100,"King");
        dt.Rows.Add(10,"Frog");

        using var bulk = tbl.BeginBulkInsert();
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

        Assert.That(ex, Is.Not.Null, "Expected upload to fail because value on row 2 is bad");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("Failed to parse value '.' in column 'score'"));
            Assert.That(ex.InnerException, Is.Not.Null, "Expected parse error to be an inner exception");
        });
        StringAssert.Contains("Could not parse string value '.' with Decider Type:DecimalTypeDecider",ex.InnerException.Message);
    }
}