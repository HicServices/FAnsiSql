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
        var tableName = nameof(CreateTable_WithAdjuster);

        var db = GetTestDatabase(type);
        ClearTable(db, ref tableName);

        var tbl = db.CreateTable(tableName,
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
#pragma warning disable CS8602 // Dereference of a possibly null reference - spurious
            if (columns[0].TypeRequested is not null) columns[0].TypeRequested.Width = 12;
#pragma warning restore CS8602 // Dereference of a possibly null reference.
        }
    }
}