using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;
using TypeGuesser;

namespace FAnsiTests.Table
{
    class DiscoverColumnsTests : DatabaseTests
    {
        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void DiscoverColumns_BadNames(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);

            var tbl = db.CreateTable("Fi ; '`sh",new[]
            {
                new DatabaseColumnRequest("Da'   ,,;ve",new DatabaseTypeRequest(typeof(string),100)), 
                new DatabaseColumnRequest("Frrrrr ##' ank",new DatabaseTypeRequest(typeof(int))) 
            });

            var cols = tbl.DiscoverColumns();
            Assert.AreEqual(2,cols.Length);

            var col = tbl.DiscoverColumn("Da'   ,,;ve");
            Assert.AreEqual(100,col.DataType.GetLengthIfString());
        }
    }
}
