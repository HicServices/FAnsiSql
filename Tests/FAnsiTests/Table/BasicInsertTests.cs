﻿using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System.Collections.Generic;
using TypeGuesser;

namespace FAnsiTests.Table
{
    class BasicInsertTests:DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer,"Dave")]
        [TestCase(DatabaseType.MySql,"Dave")]
        [TestCase(DatabaseType.Oracle, "Dave")]
        
        [TestCase(DatabaseType.MicrosoftSQLServer, @"].;\""ffff 
[")]

        [TestCase(DatabaseType.MySql, @"].;\""ffff 
[")]

        [TestCase(DatabaseType.Oracle, @"].;\""ffff 
[")]

        [TestCase(DatabaseType.MySql, "Dave")]
        [TestCase(DatabaseType.Oracle, "Dave")]
        [TestCase(DatabaseType.MicrosoftSQLServer, 1.5)]
        [TestCase(DatabaseType.MySql, 1.5)]
        [TestCase(DatabaseType.Oracle, 1.5)]
        public void CreateTableAndInsertAValue_ColumnOverload(DatabaseType type, object value)
        {
            var db = GetTestDatabase(type);
            var tbl = db.CreateTable("InsertTable",
                new []
                {
                    new DatabaseColumnRequest("Name",new DatabaseTypeRequest(value.GetType(),100,new DecimalSize(5,5)))
                });
            
            var nameCol = tbl.DiscoverColumn("Name");

            tbl.Insert(new Dictionary<DiscoveredColumn, object>()
            {
                {nameCol,value}
            });

            var result = tbl.GetDataTable();
            Assert.AreEqual(1,result.Rows.Count);
            Assert.AreEqual(value,result.Rows[0][0]);

            tbl.Drop();
        }

        [TestCase(DatabaseType.MicrosoftSQLServer, 1.5)]
        [TestCase(DatabaseType.MySql, 1.5)]
        [TestCase(DatabaseType.Oracle, 1.5)]
        public void CreateTableAndInsertAValue_StringOverload(DatabaseType type, object value)
        {
            var db = GetTestDatabase(type,true);
            var tbl = db.CreateTable("InsertTable",
                new[]
                {
                    new DatabaseColumnRequest("Name",new DatabaseTypeRequest(value.GetType(),100,new DecimalSize(5,5)))
                });

            tbl.Insert(new Dictionary<string, object>()
            {
                {"Name",value}
            });

            var result = tbl.GetDataTable();
            Assert.AreEqual(1, result.Rows.Count);
            Assert.AreEqual(value, result.Rows[0][0]);

            tbl.Drop();
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void CreateTableAndInsertAValue_ReturnsIdentity(DatabaseType type)
        {
            var db = GetTestDatabase(type,true);
            var tbl = db.CreateTable("InsertTable",
                new[]
                {
                    new DatabaseColumnRequest("myidentity",new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true,IsAutoIncrement = true}, 
                    new DatabaseColumnRequest("Name",new DatabaseTypeRequest(typeof(string),100))
                });

            var nameCol = tbl.DiscoverColumn("Name");

            int result = tbl.Insert(new Dictionary<DiscoveredColumn, object>()
            {
                {nameCol,"fish"}
            });

            Assert.AreEqual(1,result);


            result = tbl.Insert(new Dictionary<DiscoveredColumn, object>()
            {
                {nameCol,"fish"}
            });

            Assert.AreEqual(2, result);
        }
    }
}
