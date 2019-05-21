using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System;
using System.Data;

namespace FAnsiTests.Table
{
    class TopXTests :DatabaseTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer,true)]
        [TestCase(DatabaseType.MicrosoftSQLServer,false)]
        [TestCase(DatabaseType.Oracle,true)]
        [TestCase(DatabaseType.Oracle,false)]
        [TestCase(DatabaseType.MySql,true)]
        [TestCase(DatabaseType.MySql,false)]
        public void Test_TopX_OrderBy(DatabaseType type,bool asc)
        {
            var db = GetTestDatabase(type);

            DataTable dt = new DataTable();
            dt.Columns.Add("F");
            dt.Columns.Add("X");

            dt.Rows.Add(1,"fish");
            dt.Rows.Add(2,"fish");
            dt.Rows.Add(3,"fish");
            dt.Rows.Add(4,"fish");

            var tbl = db.CreateTable("MyTopXTable",dt);


            var topx = tbl.GetQuerySyntaxHelper().HowDoWeAchieveTopX(1);
            
            string sql;

            switch(topx.Location)
            {
                case QueryComponent.SELECT:
                    sql= "SELECT " + topx.SQL + " F FROM " + tbl.GetFullyQualifiedName() + " ORDER BY F " + (asc?"ASC":"DESC") ;
                    break;

                case QueryComponent.Postfix:
                    sql = "SELECT F FROM " + tbl.GetFullyQualifiedName() + " ORDER BY F "+ (asc?"ASC ":"DESC ") + topx.SQL;
                    break;
                default: throw new ArgumentOutOfRangeException("Did not expect this location");

            }

            using(var con = db.Server.GetConnection())
            {
                con.Open();
                Assert.AreEqual(asc?1:4,db.Server.GetCommand(sql,con).ExecuteScalar());
            }
        }
    }
}
