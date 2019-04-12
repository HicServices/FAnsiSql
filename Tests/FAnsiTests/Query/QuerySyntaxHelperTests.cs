using System;
using System.IO;
using System.Text.RegularExpressions;
using FAnsi;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests.Query
{
    class QuerySyntaxHelperTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        public void SyntaxHelperTest_GetRuntimeName(DatabaseType t)
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));

            var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();
            Assert.AreEqual("Frank",syntaxHelper.GetRuntimeName("count(*) as Frank"));
            Assert.AreEqual("Frank",syntaxHelper.GetRuntimeName("count(cast(1 as int)) as Frank"));
            Assert.AreEqual("Frank",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol] as Frank"));
            Assert.AreEqual("Frank",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol] as [Frank]"));
            Assert.AreEqual("Frank",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol] as `Frank`"));
            Assert.AreEqual("mycol",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol]"));
        }

        [Test]
        public void SyntaxHelperTest_GetRuntimeName_Oracle()
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));

            var syntaxHelper = ImplementationManager.GetImplementation(DatabaseType.Oracle).GetQuerySyntaxHelper();
            Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("count(*) as Frank"));
            Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("count(cast(1 as int)) as Frank"));
            Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("count(cast(1 as int)) as \"Frank\""));
            Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol] as Frank"));
            Assert.AreEqual("MYCOL",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol]"));
        }


        [TestCase("count(*) as Frank","count(*)","Frank")]
        [TestCase("count(cast(1 as int)) as Frank","count(cast(1 as int))","Frank")]
        [TestCase("[mydb].[mytbl].[mycol] as Frank","[mydb].[mytbl].[mycol]","Frank")]
        [TestCase("[mydb].[mytbl].[mycol] as [Frank]","[mydb].[mytbl].[mycol]","Frank")]
        [TestCase("[mydb].[mytbl].[mycol] as [Frank],","[mydb].[mytbl].[mycol]","Frank")]
        [TestCase("[mytbl].[mycol] AS `Frank`","[mytbl].[mycol]","Frank")]
        [TestCase("[mytbl].[mycol] AS [omg its full of spaces]","[mytbl].[mycol]","omg its full of spaces")]
        [TestCase("[mydb].[mytbl].[mycol]","[mydb].[mytbl].[mycol]",null)]
        [TestCase("[mydb].[mytbl].[mycol],","[mydb].[mytbl].[mycol]",null)]
        [TestCase("count(*) as Frank","count(*)","Frank")]
        [TestCase("count(*) as Frank32","count(*)","Frank32")]
        [TestCase("CAST([dave] as int) as [number]","CAST([dave] as int)","number")]
        [TestCase("CAST([dave] as int)","CAST([dave] as int)",null)]
        public void SyntaxHelperTest_SplitLineIntoSelectSQLAndAlias(string line, string expectedSelectSql, string expectedAlias)
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));

            foreach (DatabaseType t in new []{DatabaseType.Oracle,DatabaseType.MySql,DatabaseType.MicrosoftSQLServer})
            {
                var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();

                string selectSQL;
                string alias;
                Assert.AreEqual(expectedAlias != null,syntaxHelper.SplitLineIntoSelectSQLAndAlias(line, out selectSQL, out alias));
                Assert.AreEqual(expectedSelectSql,selectSQL);
                Assert.AreEqual(expectedAlias,alias);
            }   
        }

        [TestCase(DatabaseType.MicrosoftSQLServer)]
        [TestCase(DatabaseType.MySql)]
        [TestCase(DatabaseType.Oracle)]
        public void Test_GetAlias(DatabaseType t)
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));
            var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();
            

            if (!(syntaxHelper.AliasPrefix.StartsWith(" ") && syntaxHelper.AliasPrefix.EndsWith(" ")))
                Assert.Fail("GetAliasConst method on Type " + this.GetType().Name + " returned a value that was not bounded by whitespace ' '.  GetAliasConst must start and end with a space e.g. ' AS '");

            var testString = "col " + syntaxHelper.AliasPrefix + " bob";

            string selectSQL;
            string alias;
            syntaxHelper.SplitLineIntoSelectSQLAndAlias(testString, out selectSQL, out alias);
            
            Assert.AreEqual("col",selectSQL);
            Assert.AreEqual("bob",alias);
        }

    }
}
