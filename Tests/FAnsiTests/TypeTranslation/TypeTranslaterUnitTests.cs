using FAnsi;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal class TypeTranslaterUnitTests
{
    /// <summary>
    /// IsSupportedType is a support check for FAnsi not the DBMS.  This test shows that FAnsi's view of 'what is a string' is pretty
    /// broad.  We don't want to bind <see cref="FAnsi.Discovery.TypeTranslation.IsSupportedSQLDBType"/> to DBMS / API since that would be too brittle.
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="sqlDbType"></param>
    /// <param name="expectedOutcome"></param>
    [TestCase(DatabaseType.MicrosoftSQLServer,"varchar2(10)",true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "monkeychar7", true)]
    public void Test_IsSupportedType(DatabaseType dbType,string sqlDbType,bool expectedOutcome)
    {
        var tt = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper().TypeTranslater;
        Assert.That(tt.IsSupportedSQLDBType(sqlDbType), Is.EqualTo(expectedOutcome), $"Unexpected result for IsSupportedSQLDBType with {dbType}.  Input was '{sqlDbType}' expected {expectedOutcome}");
    }
}