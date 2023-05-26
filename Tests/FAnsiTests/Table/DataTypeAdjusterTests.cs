using System.Collections.Generic;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TableCreation;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

internal class DataTypeAdjusterTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_WithAdjuster(DatabaseType type)
    {
        var tbl = GetTestDatabase(type).CreateTable("MyTable", new[]
        {
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10))
        }, null, new DataTypeAdjusterTestsPadder());

        Assert.AreEqual(12,tbl.DiscoverColumn("Name").DataType.GetLengthIfString());
        tbl.Drop();
    }

    internal class DataTypeAdjusterTestsPadder : IDatabaseColumnRequestAdjuster
    {
        public void AdjustColumns(List<DatabaseColumnRequest> columns)
        {
            columns[0].TypeRequested.Width = 12;
        }
    }
}