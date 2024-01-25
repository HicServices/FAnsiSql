using System.Collections.Generic;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TableCreation;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

internal sealed class DataTypeAdjusterTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateTable_WithAdjuster(DatabaseType type)
    {
        var tbl = GetTestDatabase(type).CreateTable("MyTable",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof (string), 10))
        ], null, new DataTypeAdjusterTestsPadder());

        Assert.That(tbl.DiscoverColumn("Name").DataType.GetLengthIfString(), Is.EqualTo(12));
        tbl.Drop();
    }

    internal sealed class DataTypeAdjusterTestsPadder : IDatabaseColumnRequestAdjuster
    {
        public void AdjustColumns(List<DatabaseColumnRequest> columns)
        {
            columns[0].TypeRequested.Width = 12;
        }
    }
}