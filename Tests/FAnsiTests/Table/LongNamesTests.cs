using System;
using System.Collections.Generic;
using System.Text;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using NUnit.Framework;

namespace FAnsiTests.Table
{
    class LongNamesTests : DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MySql)]
        public void Test_LongTableName_CreateAndReadBack(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < db.Server.GetQuerySyntaxHelper().MaximumTableLength; i++)
                sb.Append('a');

            StringBuilder sb2 = new StringBuilder();
            for (int i = 0; i < db.Server.GetQuerySyntaxHelper().MaximumColumnLength; i++)
                sb2.Append('b');

            //128 characters long
            string tableName = sb.ToString();
            string columnName = sb2.ToString();
            
            var tbl = db.CreateTable(tableName,new DatabaseColumnRequest[]{new DatabaseColumnRequest(columnName,new DatabaseTypeRequest(typeof(string),100))});

            Assert.IsTrue(tbl.Exists());
            StringAssert.AreEqualIgnoringCase(tableName,tbl.GetRuntimeName());

            var col = tbl.DiscoverColumn(columnName);
            Assert.IsNotNull(col);
            StringAssert.AreEqualIgnoringCase(columnName,col.GetRuntimeName());
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.Oracle)]
        [TestCase(DatabaseType.MySql)]
        public void Test_LongDatabaseNames_CreateAndReadBack(DatabaseType dbType)
        {
            AssertCanCreateDatabases();

            var db = GetTestDatabase(dbType);

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < db.Server.GetQuerySyntaxHelper().MaximumDatabaseLength; i++)
                sb.Append('a');

            var db2 = db.Server.ExpectDatabase(sb.ToString());    
            db2.Create(true);
            
            Assert.IsTrue(db2.Exists());
            StringAssert.AreEqualIgnoringCase(sb.ToString(),db2.GetRuntimeName());
            
            db2.Drop();
        }
    }
}
