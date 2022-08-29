using System.Linq;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Query
{
    public class ParameterNameExtractionTests
    {
        [TestCase("a = wok.dbo.fish(@bobby)")]
        [TestCase("[bob]=@bobby")]
        [TestCase("[bob]=1+@bobby")]
        [TestCase("[bob]=1-@bobby")]
        [TestCase("[bob]=1*@bobby")]
        [TestCase("[bob]=1/@bobby")]
        [TestCase(@"[bob]=1\@bobby")]
        [TestCase("[bob]=   @bobby")]
        [TestCase("dbo.MyFunc('fish',@bobby)")]
        [TestCase("[bob]=@bobby OR [bob2]=@bobby")]
        [TestCase("[bob]=@bobby OR [bob2]=@BObby")]
        [TestCase("@bobby='active'")]
        public void TestExtractionOfParmaetersFromSQL_FindOne(string sql)
        {
            Assert.AreEqual("@bobby",QuerySyntaxHelper.GetAllParameterNamesFromQuery(sql).SingleOrDefault());
        }

        [TestCase("[bob]='@bobby'")]
        [TestCase("[bob]='myfriend@bobby.ac.uk'")]
        [TestCase("[bob]='myfriend123@bobby.ac.uk'")]
        [TestCase("[bob]=   ':bobby'")]
        public void TestExtractionOfParmaetersFromSQL_NoneOne(string sql)
        {
            Assert.AreEqual(0, QuerySyntaxHelper.GetAllParameterNamesFromQuery(sql).Count);
        }
    }
}
