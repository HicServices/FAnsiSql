﻿using System;
using System.Diagnostics;
using System.Linq;
using FAnsi;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.TypeTranslation;
using NUnit.Framework;

namespace FAnsiTests.Database
{
    class DiscoverTablesTests:DatabaseTests
    {
        [TestCase(DatabaseType.MySql)] 
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        public void Test_DiscoverTables_Normal(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);

            var tbl1 = db.CreateTable("AA",
                new DatabaseColumnRequest[]
                {
                    new DatabaseColumnRequest("F",new DatabaseTypeRequest(typeof(int)))
                });

            var tbl2 = db.CreateTable("BB",
                new DatabaseColumnRequest[]
                {
                    new DatabaseColumnRequest("F",new DatabaseTypeRequest(typeof(int)))
                });

            var tbls = db.DiscoverTables(false);

            Assert.AreEqual(2,tbls.Length);
            Assert.AreEqual(1, tbls.Count(t => t.GetRuntimeName().Equals("AA",StringComparison.CurrentCultureIgnoreCase)));
            Assert.AreEqual(1,tbls.Count(t=>t.GetRuntimeName().Equals("BB",StringComparison.CurrentCultureIgnoreCase)));

        }
        /// <summary>
        /// RDMPDEV-1548 This test explores an issue where <see cref="DiscoveredDatabase.DiscoverTables"/> would fail when
        /// there were tables in the database with invalid names.
        ///
        /// Correct behaviour is for DiscoverTables to not return any tables that have invalid names
        /// </summary>
        [TestCase(DatabaseType.MySql)] 
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        public void Test_DiscoverTables_WithInvalidNames_Skipped(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);
            
            //FAnsi doesn't let you create tables with brackets in the names so we have to do it manually
            CreateBadTable(db);
            
            //FAnsi shouldn't let us create a table with an invalid name
            Assert.Throws<RuntimeNameException>(() =>
                db.CreateTable("FF (troll)",
                    new DatabaseColumnRequest[]
                    {
                        new DatabaseColumnRequest("F", new DatabaseTypeRequest(typeof(int)))
                    }));

            //but we can create a table "FF"
            var tbl = db.CreateTable("FF",
                new DatabaseColumnRequest[]
                {
                    new DatabaseColumnRequest("F",new DatabaseTypeRequest(typeof(int)))
                });
            
            //even though there are 2 tables in the database [BB (ff)] and [FF] only [FF] should be returned
            var tbls = db.DiscoverTables(false);

            Assert.AreEqual(1,tbls.Length);
            Assert.AreEqual(1, tbls.Count(t => t.GetRuntimeName().Equals("FF",StringComparison.CurrentCultureIgnoreCase)));
            
            DropBadTable(db);
        }

        /// <summary>
        /// As above test <see cref="Test_DiscoverTables_WithInvalidNames_Skipped"/> but creates a view with a bad name instead of a table
        /// </summary>
        /// <param name="dbType"></param>
        [TestCase(DatabaseType.MySql)] 
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        public void Test_DiscoverViews_WithInvalidNames_Skipped(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);
            
            //FAnsi doesn't let you create tables with brackets in the names so we have to do it manually
            CreateBadView(db);
            
            //FAnsi shouldn't let us create a table with an invalid name
            Assert.Throws<RuntimeNameException>(() =>
                db.CreateTable("FF (troll)",
                    new DatabaseColumnRequest[]
                    {
                        new DatabaseColumnRequest("F", new DatabaseTypeRequest(typeof(int)))
                    }));

            //but we can create a table "FF"
            var tbl = db.CreateTable("FF",
                new DatabaseColumnRequest[]
                {
                    new DatabaseColumnRequest("F",new DatabaseTypeRequest(typeof(int)))
                });
            
            //even though there are 2 tables in the database [BB (ff)] and [FF] only [FF] should be returned
            var tbls = db.DiscoverTables(true);

            //should be 2 tables (and 1 bad view that doesn't get returned)
            Assert.AreEqual(2,tbls.Length);
            
            //view should not be returned because it is bad
            Assert.AreEqual(0,tbls.Count(t=>t.TableType == TableType.View));
            Assert.AreEqual(1, tbls.Count(t => t.GetRuntimeName().Equals("FF",StringComparison.CurrentCultureIgnoreCase)));
            
            DropBadView(db);
        }

        private void DropBadTable(DiscoveredDatabase db)
        {
            using(var con = db.Server.GetConnection())
            {
                con.Open();
                var cmd = db.Server.GetCommand($"DROP TABLE {GetBadTableName(db)}",con);
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Exception)
                {
                    Console.WriteLine("Drop table failed, this is expected, since FAnsi won't see this dodgy table name we can't drop it as normal before tests");
                }
            }
        }

        private string GetBadTableName(DiscoveredDatabase db)
        {
            switch (db.Server.DatabaseType)
            {
                case DatabaseType.MicrosoftSQLServer:
                    return "[BB (ff)]";
                case DatabaseType.MySql:
                    return "`BB (ff)`";
                case DatabaseType.Oracle:
                    return  db.GetRuntimeName() + ".\"BB (ff)\"";
                default:
                    throw new ArgumentOutOfRangeException(nameof(db.Server.DatabaseType), db.Server.DatabaseType, null);
            }
        }

        private void CreateBadTable(DiscoveredDatabase db)
        {
            //drop it if it exists
            DropBadTable(db);

            using(var con = db.Server.GetConnection())
            {
                con.Open();
                var cmd = db.Server.GetCommand($"CREATE TABLE {GetBadTableName(db)} (A int not null)",con);
                cmd.ExecuteNonQuery();
            }
        }


        private void DropBadView(DiscoveredDatabase db)
        {
            //the table that the view reads from
            var abc = db.ExpectTable("ABC");
            if(abc.Exists())
                abc.Drop();
            
            using(var con = db.Server.GetConnection())
            {
                con.Open();
                var cmd = db.Server.GetCommand($"DROP VIEW {GetBadTableName(db)}",con);
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Exception)
                {
                    Console.WriteLine("Drop view failed, this is expected, since FAnsi won't see this dodgy table name we can't drop it as normal before tests");
                }
            }
        }


        private void CreateBadView(DiscoveredDatabase db)
        {
            //drop it if it exists
            DropBadView(db);

            db.CreateTable("ABC",new DatabaseColumnRequest[]{new DatabaseColumnRequest("A",new DatabaseTypeRequest(typeof(int)))});

            using(var con = db.Server.GetConnection())
            {
                con.Open();
                var cmd = db.Server.GetCommand($"CREATE VIEW {GetBadTableName(db)} as select * from ABC",con);
                cmd.ExecuteNonQuery();
            }
        }
    }
}