using FAnsi;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation
{
    class TypeTranslaterUnitTests
    {
        /// <summary>
        /// IsSupportedType is a support check for FAnsi not the DBMS.  This test shows that FAnsi's view of 'what is a string' is pretty
        /// broad.  We don't want to bind <see cref="IsSupportedSQLDBType"/> to DBMS / API since that would be too brittle.
        /// </summary>
        /// <param name="dbType"></param>
        /// <param name="sqlDbType"></param>
        /// <param name="expectedOutcome"></param>
        [TestCase(DatabaseType.MicrosoftSQLServer,"varchar2(10)",true)]
        [TestCase(DatabaseType.MicrosoftSQLServer, "monkeychar7", true)]
        public void Test_IsSupportedType(DatabaseType dbType,string sqlDbType,bool expectedOutcome)
        {
            ImplementationManager.Load<OracleImplementation>();
            ImplementationManager.Load<MicrosoftSQLImplementation>();
            ImplementationManager.Load<MySqlImplementation>();

            var tt = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper().TypeTranslater;

            Assert.AreEqual(expectedOutcome,tt.IsSupportedSQLDBType(sqlDbType),$"Unexpected result for IsSupportedSQLDBType with {dbType}.  Input was '{sqlDbType}' expected {expectedOutcome}");
        }
    }
}
