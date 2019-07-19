using FAnsi;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;

namespace FAnsiTests.TypeTranslation
{
    class TypeTranslaterUnitTests
    {
        [TestCase(DatabaseType.MicrosoftSQLServer,"varchar2(10)",false)]
        [TestCase(DatabaseType.MicrosoftSQLServer, "monkeychar7", false)]
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
