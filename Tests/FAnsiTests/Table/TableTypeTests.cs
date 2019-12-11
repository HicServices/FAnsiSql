using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System;
using System.Data;

namespace FAnsiTests.Table
{
    public class TableTypeTests:DatabaseTests
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

            Assert.AreEqual(TableType.Table, tbl.TableType);

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
            var view = tbl.Database.ExpectTable("MyView",null);
            Assert.IsFalse(view.Exists()); //we should be wrong

            //if we expect it to be a view
            view = tbl.Database.ExpectTable("MyView",null,TableType.View);

            Assert.AreEqual(1,view.DiscoverColumns().Length);

            //we would be right!
            Assert.IsTrue(view.Exists());
            Assert.AreEqual(TableType.View,view.TableType);

            view.Drop();
            Assert.IsFalse(view.Exists());

            var ex = Assert.Throws<NotSupportedException>(()=>view.Rename("Lolz"));
            Assert.AreEqual("Rename is not supported for TableType View", ex.Message);

        }
    }
}
