using System;
using System.IO;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests.Query;

internal class QuerySyntaxHelperTests
{


    //Oracle always uppers everything because... Oracle
    [TestCase(DatabaseType.Oracle,"CHI","\"TEST_ScratchArea\".public.\"Biochemistry\".\"chi\"")]
    [TestCase(DatabaseType.PostgreSql,"chi","\"TEST_ScratchArea\".public.\"Biochemistry\".\"chi\"")]

    [TestCase(DatabaseType.Oracle,"FRANK","count(*) as Frank")]
    [TestCase(DatabaseType.PostgreSql,"Frank","count(*) as Frank")]

    [TestCase(DatabaseType.Oracle,"FRANK","count(cast(1 as int)) as Frank")]
    [TestCase(DatabaseType.PostgreSql,"Frank","count(cast(1 as int)) as Frank")]

    [TestCase(DatabaseType.Oracle,"FRANK","count(cast(1 as int)) as \"Frank\"")]
    [TestCase(DatabaseType.PostgreSql,"Frank","count(cast(1 as int)) as \"Frank\"")]
    [TestCase(DatabaseType.MySql,"Frank","count(cast(1 as int)) as `Frank`")]
    [TestCase(DatabaseType.MicrosoftSQLServer,"Frank","count(cast(1 as int)) as [Frank]")]

    [TestCase(DatabaseType.Oracle,"FRANK","[mydb].[mytbl].[mycol] as Frank")]
    [TestCase(DatabaseType.PostgreSql,"Frank","[mydb].[mytbl].[mycol] as Frank")]
    [TestCase(DatabaseType.MicrosoftSQLServer,"Frank","[mydb].[mytbl].[mycol] as Frank")]
    [TestCase(DatabaseType.MySql,"Frank","[mydb].[mytbl].[mycol] as Frank")]

    [TestCase(DatabaseType.Oracle,"ZOMBIE","dbo.GetMyCoolThing(\"Magic Fun Times\") as zombie")]
    [TestCase(DatabaseType.MicrosoftSQLServer,"zombie","dbo.GetMyCoolThing(\"Magic Fun Times\") as zombie")]
    [TestCase(DatabaseType.MySql,"zombie","dbo.GetMyCoolThing(\"Magic Fun Times\") as zombie")]
    [TestCase(DatabaseType.PostgreSql,"zombie","dbo.GetMyCoolThing(\"Magic Fun Times\") as zombie")]

    [TestCase(DatabaseType.Oracle,"MYCOL","\"mydb\".\"mytbl\".\"mycol\"")]
    [TestCase(DatabaseType.MicrosoftSQLServer,"mycol","[mydb].[mytbl].[mycol]")]
    [TestCase(DatabaseType.MySql,"mycol","`mydb`.`mytbl`.`mycol`")]
    [TestCase(DatabaseType.PostgreSql,"mycol","\"mydb\".\"mytbl\".\"mycol\"")]
    public void SyntaxHelperTest_GetRuntimeName(DatabaseType dbType,  string expected, string forInput)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();
        Assert.AreEqual(expected,syntaxHelper.GetRuntimeName(forInput));
    }

    /// <summary>
    /// Tests that no matter how many times you call EnsureWrapped or GetRuntimeName you always end up with the format that matches the last method call
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="runtime"></param>
    /// <param name="wrapped"></param>
    [TestCase(DatabaseType.MySql,"Fra`nk","`Fra``nk`")]
    [TestCase(DatabaseType.MySql,"Fra``nk`","`Fra````nk```")]
    [TestCase(DatabaseType.MicrosoftSQLServer,"Fra]nk","[Fra]]nk]")]
    [TestCase(DatabaseType.MicrosoftSQLServer,"Fra]]nk]","[Fra]]]]nk]]]")]
    [TestCase(DatabaseType.PostgreSql,"Fra\"nk","\"Fra\"\"nk\"")]
    [TestCase(DatabaseType.PostgreSql,"Fra\"\"nk\"","\"Fra\"\"\"\"nk\"\"\"")]
    public void SyntaxHelperTest_GetRuntimeName_MultipleCalls(DatabaseType dbType,  string runtime, string wrapped)
    {
        // NOTE: Oracle does not support such shenanigans https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm
        // "neither quoted nor unquoted identifiers can contain double quotation marks or the null character (\0)."

        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

        var currentName = runtime;

        for(var i=0;i<10;i++)
        {
            if(i%2 ==0 )
            {
                Assert.AreEqual(runtime,currentName);
                currentName = syntaxHelper.EnsureWrapped(currentName);
                currentName = syntaxHelper.EnsureWrapped(currentName);
                currentName = syntaxHelper.EnsureWrapped(currentName);
            }
            else
            {
                Assert.AreEqual(wrapped,currentName);
                currentName = syntaxHelper.GetRuntimeName(currentName);
                currentName = syntaxHelper.GetRuntimeName(currentName);
                currentName = syntaxHelper.GetRuntimeName(currentName);
            }
        }
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void EnsureWrapped_MultipleCalls(DatabaseType dbType)
    {
        var syntax = new QuerySyntaxHelperFactory().Create(dbType);

        var once = syntax.EnsureWrapped("ff");
        var twice = syntax.EnsureWrapped(once);

        Assert.AreEqual(once,twice);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void SyntaxHelperTest_GetRuntimeName_Impossible(DatabaseType t)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();
        var ex = Assert.Throws<RuntimeNameException>(()=>syntaxHelper.GetRuntimeName("count(*)"));
        StringAssert.Contains("Could not determine runtime name for Sql:'count(*)'.  It had brackets and no alias.",ex?.Message);

        Assert.Throws<RuntimeNameException>(()=>syntaxHelper.GetRuntimeName("dbo.GetMyCoolThing(\"Magic Fun Times\")"));

        Assert.IsFalse(syntaxHelper.TryGetRuntimeName("count(*)",out _));
        Assert.IsFalse(syntaxHelper.TryGetRuntimeName("dbo.GetMyCoolThing(\"Magic Fun Times\")",out _));
    }

    [Test]
    public void SyntaxHelperTest_GetRuntimeName_Oracle()
    {
        var syntaxHelper = ImplementationManager.GetImplementation(DatabaseType.Oracle).GetQuerySyntaxHelper();
        Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("count(*) as Frank"));
        Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("count(cast(1 as int)) as Frank"));
        Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("count(cast(1 as int)) as \"Frank\""));
        Assert.AreEqual("FRANK",syntaxHelper.GetRuntimeName("\"mydb\".\"mytbl\".\"mycol\" as Frank"));
        Assert.AreEqual("MYCOL",syntaxHelper.GetRuntimeName("\"mydb\".\"mytbl\".\"mycol\""));
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
        foreach (var t in new []{DatabaseType.Oracle,DatabaseType.MySql,DatabaseType.MicrosoftSQLServer})
        {
            var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();

            Assert.AreEqual(expectedAlias != null,syntaxHelper.SplitLineIntoSelectSQLAndAlias(line, out var selectSQL, out var alias));
            Assert.AreEqual(expectedSelectSql,selectSQL);
            Assert.AreEqual(expectedAlias,alias);
        }   
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_GetAlias(DatabaseType t)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();

        if (!(syntaxHelper.AliasPrefix.StartsWith(" ") && syntaxHelper.AliasPrefix.EndsWith(" ")))
            Assert.Fail(
                $"GetAliasConst method on Type {GetType().Name} returned a value that was not bounded by whitespace ' '.  GetAliasConst must start and end with a space e.g. ' AS '");

        var testString = $"col {syntaxHelper.AliasPrefix} bob";

        syntaxHelper.SplitLineIntoSelectSQLAndAlias(testString, out var selectSQL, out var alias);
            
        Assert.AreEqual("col",selectSQL);
        Assert.AreEqual("bob",alias);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_NameValidation(DatabaseType dbType)
    {
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

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetFullyQualifiedName_WhitespaceSchema(DatabaseType dbType)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

        foreach(var emptySchemaExpression in new [] { null,"", " ", "\t"})
        {
            var name = syntaxHelper.EnsureFullyQualified("mydb", emptySchemaExpression, "Troll", "MyCol");
            Assert.IsTrue(string.Equals("MyCol", syntaxHelper.GetRuntimeName(name),StringComparison.InvariantCultureIgnoreCase));

            switch (dbType)
            {
                case DatabaseType.MicrosoftSQLServer:
                    Assert.AreEqual("[mydb]..[Troll].[MyCol]",name);
                    break;
                case DatabaseType.MySql:
                    Assert.AreEqual("`mydb`.`Troll`.`MyCol`", name);
                    break;
                case DatabaseType.Oracle:
                    Assert.AreEqual("\"MYDB\".\"TROLL\".\"MYCOL\"", name);
                    break;
                case DatabaseType.PostgreSql:
                    Assert.AreEqual("\"mydb\".public.\"Troll\".\"MyCol\"", name);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null);
            }
        }

    }

    [Test]
    public void Test_GetFullyQualifiedName_BacktickMySql()
    {
        var syntaxHelper = ImplementationManager.GetImplementation(DatabaseType.MySql).GetQuerySyntaxHelper();

        //when names have backticks the correct response is to double back tick them
        Assert.AreEqual("`ff``ff`",syntaxHelper.EnsureWrapped("ff`ff"));
        Assert.AreEqual("`d``b`.`ta``ble`",syntaxHelper.EnsureFullyQualified("d`b",null,"ta`ble"));

        //runtime name should still be the actual name of the column
        Assert.AreEqual("ff`ff",syntaxHelper.GetRuntimeName("ff`ff"));
    }
}