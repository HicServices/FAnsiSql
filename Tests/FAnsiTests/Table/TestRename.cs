using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

internal class TestRename:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void TestRenamingTable(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyTable",[new DatabaseColumnRequest("Age",new DatabaseTypeRequest(typeof(int)) )]);

        Assert.That(tbl.Exists());

        var tbl2 = db.ExpectTable("MYTABLE2");
        Assert.That(tbl2.Exists(), Is.False);

        tbl.Rename("MYTABLE2");

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists());
            Assert.That(tbl2.Exists());

            Assert.That(tbl.GetRuntimeName(), Is.EqualTo("MYTABLE2"));
            Assert.That(tbl2.GetRuntimeName(), Is.EqualTo("MYTABLE2"));
        });

    }
}