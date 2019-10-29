using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Query
{
    class QuerySyntaxHelperDatabaseTests : DatabaseTests
    {
        
        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void Test_HowDoWeAchieveMd5(DatabaseType dbType)
        {
            var db = GetTestDatabase(dbType,false);

            string sql = "SELECT " + db.Server.GetQuerySyntaxHelper().HowDoWeAchieveMd5("'fish'");


            //because Oracle :)
            if (dbType == DatabaseType.Oracle)
                sql += " FROM dual";

            using (var con = db.Server.GetConnection())
            {
                con.Open();

                var result = db.Server.GetCommand(sql, con).ExecuteScalar();

                StringAssert.AreEqualIgnoringCase("83E4A96AED96436C621B9809E258B309",result.ToString());
            }
        }
    }
}