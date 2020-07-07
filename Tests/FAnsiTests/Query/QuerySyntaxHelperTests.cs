using System;
using System.IO;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests.Query
{
    class QuerySyntaxHelperTests
    {
        //Oracle always uppers everything because... Oracle
        [TestCase(DatabaseType.Oracle,true)]
        [TestCase(DatabaseType.MicrosoftSQLServer,false)]
        [TestCase(DatabaseType.MySql,false)]
        [TestCase(DatabaseType.PostgreSql,false)]
        public void SyntaxHelperTest_GetRuntimeName(DatabaseType t,bool expectUpper)
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));

            var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();

            Assert.AreEqual(expectUpper?"CHI":"chi",syntaxHelper.GetRuntimeName("\"TEST_ScratchArea\".public.\"Biochemistry\".\"chi\""));

            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("count(*) as Frank"));
            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("count(cast(1 as int)) as Frank"));
            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol] as Frank"));
            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol] as [Frank]"));
            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("\"Mydb\".\"mytbl\".\"mycol\" as \"Frank\""));
            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol] as `Frank`"));
            Assert.AreEqual(expectUpper?"MYCOL":"mycol",syntaxHelper.GetRuntimeName("[mydb].[mytbl].[mycol]"));
            Assert.AreEqual(expectUpper?"ZOMBIE":"zombie",syntaxHelper.GetRuntimeName("dbo.GetMyCoolThing(\"Magic Fun Times\") as zombie"));
                        
            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("`mydb`.`mytbl`.`mycol` as `Frank`"));

            Assert.AreEqual(expectUpper?"FRANK":"Frank",syntaxHelper.GetRuntimeName("\"mydb\".\"mytbl\".\"mycol\" as \"Frank\""));

            
            Assert.IsTrue(syntaxHelper.TryGetRuntimeName("\"mydb\".\"mytbl\".\"mycol\" as \"Frank\"",out string name));
            Assert.AreEqual(expectUpper?"FRANK":"Frank",name);
        }
        
        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void SyntaxHelperTest_GetRuntimeName_Impossible(DatabaseType t)
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));

            var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();
            var ex = Assert.Throws<RuntimeNameException>(()=>syntaxHelper.GetRuntimeName("count(*)"));
            StringAssert.Contains("Could not determine runtime name for Sql:'count(*)'.  It had brackets and no alias.",ex.Message);

            Assert.Throws<RuntimeNameException>(()=>syntaxHelper.GetRuntimeName("dbo.GetMyCoolThing(\"Magic Fun Times\")"));

            Assert.IsFalse(syntaxHelper.TryGetRuntimeName("count(*)",out _));
            Assert.IsFalse(syntaxHelper.TryGetRuntimeName("dbo.GetMyCoolThing(\"Magic Fun Times\")",out _));
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

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
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

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void Test_NameValidation(DatabaseType dbType)
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));
            var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

            Assert.Throws<RuntimeNameException>(()=>syntaxHelper.ValidateDatabaseName(null));
            Assert.Throws<RuntimeNameException>(()=>syntaxHelper.ValidateDatabaseName("  "));
            Assert.Throws<RuntimeNameException>(()=>syntaxHelper.ValidateDatabaseName("db.table"));
            Assert.Throws<RuntimeNameException>(()=>syntaxHelper.ValidateDatabaseName("db(lol)"));
            Assert.Throws<RuntimeNameException>(()=>syntaxHelper.ValidateDatabaseName(new string('A', syntaxHelper.MaximumDatabaseLength+1)));

            Assert.DoesNotThrow(()=>syntaxHelper.ValidateDatabaseName("A"));
            Assert.DoesNotThrow(()=>syntaxHelper.ValidateDatabaseName(new string('A', syntaxHelper.MaximumDatabaseLength)));
        }

        [Test]
        public void Test_MakeHeaderNameSensible_Unicode()
        {
            //normal unicode is fine
            Assert.AreEqual("你好", QuerySyntaxHelper.MakeHeaderNameSensible("你好"));
            Assert.AreEqual("你好DropDatabaseBob", QuerySyntaxHelper.MakeHeaderNameSensible("你好; drop database bob;"));
        }

        [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
        public void Test_GetFullyQualifiedName(DatabaseType dbType)
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));
            var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

            var name = syntaxHelper.EnsureFullyQualified("mydb", null, "Troll", ",,,");
            Assert.AreEqual(",,,",syntaxHelper.GetRuntimeName(name));

            switch (dbType)
            {
                case DatabaseType.MicrosoftSQLServer:
                    Assert.AreEqual("[mydb]..[Troll].[,,,]",name);
                    break;
                case DatabaseType.MySql:
                    Assert.AreEqual("`mydb`.`Troll`.`,,,`",name);
                    break;
                case DatabaseType.Oracle:
                    Assert.AreEqual("\"MYDB\".\"TROLL\".\",,,\"",name);
                    break;
                case DatabaseType.PostgreSql:
                    Assert.AreEqual("\"mydb\".public.\"Troll\".\",,,\"",name);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null);
            }
        }
        
        [Test]
        public void Test_GetFullyQualifiedName_BacktickMySql()
        {
            ImplementationManager.Load(new DirectoryInfo(TestContext.CurrentContext.TestDirectory));
            var syntaxHelper = ImplementationManager.GetImplementation(DatabaseType.MySql).GetQuerySyntaxHelper();

            //when names have backticks the correct response is to double back tick them
            Assert.AreEqual("`ff``ff`",syntaxHelper.EnsureWrapped("ff`ff"));
            Assert.AreEqual("`d``b`.`ta``ble`",syntaxHelper.EnsureFullyQualified("d`b",null,"ta`ble"));
        }
    }
}
