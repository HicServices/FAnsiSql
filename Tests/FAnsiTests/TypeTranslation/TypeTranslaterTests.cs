using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Exceptions;
using FAnsi.Implementation;
using FansiTests;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation
{
    /// <summary>
    /// <para>These tests cover the systems ability to match database provider specific data types to a C# Type.</para>
    /// 
    /// <para>This is further complicated since DBMS datatypes have aliases e.g. BOOL,BOOLEAN and tinyint(1) are all aliases
    /// for the same thing in MySql (the DBMS will create a tinyint(1)).</para>
    /// 
    /// <para>These tests also create tables called TTT in the test database and test the systems ability to discover the column
    /// and reverse engineer the original data type from the database.</para>
    /// </summary>
    public class TypeTranslaterTests : DatabaseTests
    {
        readonly Dictionary<DatabaseType,ITypeTranslater> _translaters = new Dictionary<DatabaseType, ITypeTranslater>();
        
        [OneTimeSetUp]
        public void SetupDatabases()
        {
            foreach (DatabaseType type in Enum.GetValues(typeof(DatabaseType)))
            {
                try
                {
                    var tt = ImplementationManager.GetImplementation(type).GetQuerySyntaxHelper().TypeTranslater;
                    _translaters.Add(type,tt);
                }
                catch (ImplementationNotFoundException)
                {
                    //no implementation for this Type
                }
            }
        }

        [TestCase(DatabaseType.MicrosoftSQLServer,"varchar(10)")]
        [TestCase(DatabaseType.MySql, "varchar(10)")]
        [TestCase(DatabaseType.Oracle, "varchar2(10)")]
        public void Test_CSharpToDbType_String10(DatabaseType type,string expectedType)
        {
            var cSharpType = new DatabaseTypeRequest(typeof (string), 10, null);

            Assert.AreEqual(expectedType,_translaters[type].GetSQLDBTypeForCSharpType(cSharpType));}

        [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(max)")]
        [TestCase(DatabaseType.MySql, "text")]
        [TestCase(DatabaseType.Oracle, "CLOB")]
        public void Test_CSharpToDbType_StringMax(DatabaseType type,string expectedType)
        {
            var cSharpType = new DatabaseTypeRequest(typeof(string), 10000000, null);
            
            //Does a request for a max length string give the expected data type?
            Assert.AreEqual(expectedType,_translaters[type].GetSQLDBTypeForCSharpType(cSharpType));

            //Does the TypeTranslater know that this datatype has no limit on characters?
            Assert.AreEqual(int.MaxValue, _translaters[type].GetLengthIfString(expectedType));

            //And does the TypeTranslater know that this datatype is string
            Assert.AreEqual(typeof(string), _translaters[type].GetCSharpTypeForSQLDBType(expectedType));
        }
        
        [TestCase(DatabaseType.MicrosoftSQLServer,"bigint")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"binary")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"bit")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"char")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"date")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"datetime")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"datetime2")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"datetimeoffset")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"decimal")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"varbinary(max)")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"float")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"image")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"int")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"money")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"nchar")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"ntext")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"numeric")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"nvarchar")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"real")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"rowversion")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"smalldatetime")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"smallint")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"smallmoney")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"text")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"time")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"timestamp")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"tinyint")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"uniqueidentifier")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"varbinary")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"varchar")]
        [TestCase(DatabaseType.MicrosoftSQLServer, "xml")]
        [TestCase(DatabaseType.MySql, "BOOL")]
        [TestCase(DatabaseType.MySql, "BOOLEAN")]
        [TestCase(DatabaseType.MySql, "TINYINT")]
        [TestCase(DatabaseType.MySql, "CHARACTER VARYING(10)")]
        [TestCase(DatabaseType.MySql, "FIXED")]
        [TestCase(DatabaseType.MySql, "DEC")]
        [TestCase(DatabaseType.MySql, "VARCHAR(10)")]
        [TestCase(DatabaseType.MySql, "DECIMAL")]
        [TestCase(DatabaseType.MySql, "FLOAT4")]
        [TestCase(DatabaseType.MySql, "FLOAT")]
        [TestCase(DatabaseType.MySql, "FLOAT8")]
        [TestCase(DatabaseType.MySql, "DOUBLE")]
        [TestCase(DatabaseType.MySql, "INT1")]
        [TestCase(DatabaseType.MySql, "INT2")]
        [TestCase(DatabaseType.MySql, "INT3")]
        [TestCase(DatabaseType.MySql, "INT4")]
        [TestCase(DatabaseType.MySql, "INT8")]
        [TestCase(DatabaseType.MySql, "SMALLINT")]
        [TestCase(DatabaseType.MySql, "MEDIUMINT")]
        [TestCase(DatabaseType.MySql, "INT")]
        [TestCase(DatabaseType.MySql, "BIGINT")]
        [TestCase(DatabaseType.MySql, "LONG VARBINARY")]
        [TestCase(DatabaseType.MySql, "MEDIUMBLOB")]
        [TestCase(DatabaseType.MySql, "LONG VARCHAR")]
        [TestCase(DatabaseType.MySql, "MEDIUMTEXT")]
        [TestCase(DatabaseType.MySql, "LONG")]
        [TestCase(DatabaseType.MySql, "MIDDLEINT")]
        [TestCase(DatabaseType.MySql, "NUMERIC")]
        [TestCase(DatabaseType.MySql, "INTEGER")]
        [TestCase(DatabaseType.MySql, "BIT")]
        [TestCase(DatabaseType.MySql, "SMALLINT(3)")]
        [TestCase(DatabaseType.MySql, "INT UNSIGNED")]
        [TestCase(DatabaseType.MySql, "INT UNSIGNED ZEROFILL")]
        [TestCase(DatabaseType.MySql, "SMALLINT UNSIGNED")]
        [TestCase(DatabaseType.MySql, "SMALLINT ZEROFILL UNSIGNED")]
        [TestCase(DatabaseType.MySql, "LONGTEXT")]
        [TestCase(DatabaseType.MySql, "CHAR(10)")]
        [TestCase(DatabaseType.MySql, "TEXT")]
        [TestCase(DatabaseType.MySql, "BLOB")]
        [TestCase(DatabaseType.MySql, "ENUM('fish','carrot')")]
        [TestCase(DatabaseType.MySql, "SET('fish','carrot')")]
        [TestCase(DatabaseType.MySql, "VARBINARY(10)")]
        [TestCase(DatabaseType.MySql, "date")]
        [TestCase(DatabaseType.MySql, "datetime")]
        [TestCase(DatabaseType.MySql, "TIMESTAMP")]
        [TestCase(DatabaseType.MySql, "TIME")]
        [TestCase(DatabaseType.MySql, "nchar")]
        [TestCase(DatabaseType.MySql, "nvarchar(10)")]
        [TestCase(DatabaseType.MySql, "real")]
        [TestCase(DatabaseType.Oracle, "varchar2(10)")]
        public void TestIsKnownType_Microsoft(DatabaseType databaseType,string sqlType)
        {
            RunKnownTypeTest(databaseType, sqlType);
        }


        private void RunKnownTypeTest(DatabaseType type, string sqlType)
        {
            var db = GetTestDatabase(type);
            var tt = db.Server.GetQuerySyntaxHelper().TypeTranslater;

            var tBefore = tt.GetCSharpTypeForSQLDBType(sqlType);
            
            Assert.IsNotNull(tBefore);

            var tbl = db.CreateTable("TTT", new[] { new DatabaseColumnRequest("MyCol", sqlType) });

            try
            {
                var col = tbl.DiscoverColumns().Single();
                var datatypeComputer = col.GetDataTypeComputer();
                Assert.IsNotNull(datatypeComputer.CurrentEstimate);
                var tAfter = datatypeComputer.CurrentEstimate;

                Assert.AreEqual(tBefore, tAfter);

                if(!string.Equals(col.DataType.SQLType,sqlType,StringComparison.CurrentCultureIgnoreCase))
                    Console.WriteLine("{0} created a '{1}' when asked to create a '{2}'",type,col.DataType.SQLType,sqlType);

            }
            finally
            {
                tbl.Drop();
            }   
        }

        //Data types not supported by FAnsi
        [TestCase(DatabaseType.MySql,"GEOMETRY")]
        [TestCase(DatabaseType.MySql,"POINT")]
        [TestCase(DatabaseType.MySql,"LINESTRING")]
        [TestCase(DatabaseType.MySql,"POLYGON")]
        [TestCase(DatabaseType.MySql,"MULTIPOINT")]
        [TestCase(DatabaseType.MySql,"MULTILINESTRING")]
        [TestCase(DatabaseType.MySql,"MULTIPOLYGON")]
        [TestCase(DatabaseType.MySql,"GEOMETRYCOLLECTION")]
        [TestCase(DatabaseType.MicrosoftSQLServer,"sql_variant")]
        public void TestNotSupportedTypes(DatabaseType type, string sqlType)
        {
            Assert.IsFalse(_translaters[type].IsSupportedSQLDBType(sqlType));
        }
    }
}