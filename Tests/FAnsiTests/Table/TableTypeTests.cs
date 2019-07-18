using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace FAnsiTests.Table
{
    public class TableTypeTests:DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void CreateView(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType);
            
            var dt = new DataTable();
            dt.Columns.Add("FF");
            
            var tbl = db.CreateTable("MyTable",dt);

            Assert.AreEqual(TableType.Table, tbl.TableType);

            var viewName = "MyView";

            //oracle likes to create stuff under your user account not the database your actually using!
            if(dbType == DatabaseType.Oracle)
            {
                var syntax = tbl.GetQuerySyntaxHelper();
                viewName = syntax.EnsureFullyQualified(tbl.Database.GetRuntimeName(),null,"MyView");
            }
            
            var sql = string.Format(@"CREATE VIEW {0} AS
SELECT FF
FROM {1}",
viewName,
 tbl.GetFullyQualifiedName());

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

            //we would be right!
            Assert.IsTrue(view.Exists());
            Assert.AreEqual(TableType.View,view.TableType);

            view.Drop();
            Assert.IsFalse(view.Exists());

            view.Rename("Lolz");

        }
    }
}
