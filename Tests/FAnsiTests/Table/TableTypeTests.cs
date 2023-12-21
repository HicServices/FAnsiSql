using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System;
using System.Data;

namespace FAnsiTests.Table;

public sealed class TableTypeTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void CreateView(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);
        DiscoveredTable tbl;

        using (var dt = new DataTable())
        {
            dt.Columns.Add("FF");
            tbl = db.CreateTable("MyTable",dt);
        }

        Assert.That(tbl.TableType, Is.EqualTo(TableType.Table));

        var viewName = "MyView";

        var syntax = tbl.GetQuerySyntaxHelper();

        //oracle likes to create stuff under your user account not the database your actually using!
        if(dbType == DatabaseType.Oracle)
        {
            viewName = syntax.EnsureFullyQualified(tbl.Database.GetRuntimeName(),null,"MyView");
        }

        var sql = string.Format(@"CREATE VIEW {0} AS
SELECT {2}
FROM {1}",
            dbType == DatabaseType.Oracle ? viewName : syntax.EnsureWrapped(viewName),
            tbl.GetFullyQualifiedName(),
            syntax.EnsureWrapped("FF")
        );

        using(var con = tbl.Database.Server.GetConnection())
        {
            con.Open();

            var cmd = tbl.GetCommand(sql,con);
            cmd.ExecuteNonQuery();
        }

        //if we expect it to be a table
        var view = tbl.Database.ExpectTable("MyView");
        Assert.That(view.Exists(), Is.False); //we should be wrong

        //if we expect it to be a view
        view = tbl.Database.ExpectTable("MyView",null,TableType.View);

        Assert.Multiple(() =>
        {
            Assert.That(view.DiscoverColumns(), Has.Length.EqualTo(1));

            //we would be right!
            Assert.That(view.Exists());
            Assert.That(view.TableType, Is.EqualTo(TableType.View));
        });

        view.Drop();
        Assert.That(view.Exists(), Is.False);

        var ex = Assert.Throws<NotSupportedException>(()=>view.Rename("Lolz"));
        Assert.That(ex?.Message, Is.EqualTo("Rename is not supported for TableType View"));

    }
}