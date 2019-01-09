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
        
        [TestCase(DatabaseType.MicrosoftSQLServer,"bigint",typeof(int))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"binary",typeof(byte[]))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"bit",typeof(bool))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"char",typeof(string))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"date",typeof(DateTime))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"datetime",typeof(DateTime))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"datetime2",typeof(DateTime))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"datetimeoffset",typeof(DateTime))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"decimal", typeof(decimal))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"varbinary(max)", typeof(byte[]))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"float",typeof(decimal))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"image",typeof(byte[]))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"int",typeof(int))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"money",typeof(decimal))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"nchar",typeof(string))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"ntext",typeof(string))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"numeric",typeof(decimal))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"nvarchar",typeof(string))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"real",typeof(decimal))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"rowversion",typeof(byte[]))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"smalldatetime",typeof(DateTime))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"smallint",typeof(short))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"smallmoney",typeof(decimal))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"text",typeof(string))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"time",typeof(TimeSpan))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"timestamp",typeof(byte[]))] //yup thats right: https://stackoverflow.com/questions/7105093/difference-between-datetime-and-timestamp-in-sqlserver
        [TestCase(DatabaseType.MicrosoftSQLServer,"tinyint",typeof(byte))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"uniqueidentifier",typeof(Guid))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"varbinary",typeof(byte[]))]
        [TestCase(DatabaseType.MicrosoftSQLServer,"varchar",typeof(string))]
        [TestCase(DatabaseType.MicrosoftSQLServer, "xml",typeof(string))]
        [TestCase(DatabaseType.MySql, "BOOL",typeof(bool))]
        [TestCase(DatabaseType.MySql, "BOOLEAN",typeof(bool))]
        [TestCase(DatabaseType.MySql, "TINYINT",typeof(byte))]
        [TestCase(DatabaseType.MySql, "CHARACTER VARYING(10)",typeof(string))]
        [TestCase(DatabaseType.MySql, "FIXED",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "DEC",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "VARCHAR(10)",typeof(string))]
        [TestCase(DatabaseType.MySql, "DECIMAL",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "FLOAT4",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "FLOAT",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "FLOAT8",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "DOUBLE",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "INT1",typeof(byte))]
        [TestCase(DatabaseType.MySql, "INT2",typeof(short))]
        [TestCase(DatabaseType.MySql, "INT3",typeof(int))]
        [TestCase(DatabaseType.MySql, "INT4",typeof(int))]
        [TestCase(DatabaseType.MySql, "INT8",typeof(long))]
        [TestCase(DatabaseType.MySql, "SMALLINT",typeof(short))]
        [TestCase(DatabaseType.MySql, "MEDIUMINT",typeof(int))]
        [TestCase(DatabaseType.MySql, "INT",typeof(int))]
        [TestCase(DatabaseType.MySql, "BIGINT",typeof(long))]
        [TestCase(DatabaseType.MySql, "LONG VARBINARY",typeof(byte[]))]
        [TestCase(DatabaseType.MySql, "MEDIUMBLOB",typeof(byte[]))]
        [TestCase(DatabaseType.MySql, "LONG VARCHAR",typeof(string))]
        [TestCase(DatabaseType.MySql, "MEDIUMTEXT",typeof(string))]
        [TestCase(DatabaseType.MySql, "LONG", typeof(string))] //yes in MySql LONG is text (https://dev.mysql.com/doc/refman/8.0/en/other-vendor-data-types.html)
        [TestCase(DatabaseType.MySql, "MIDDLEINT",typeof(int))]
        [TestCase(DatabaseType.MySql, "NUMERIC",typeof(decimal))]
        [TestCase(DatabaseType.MySql, "INTEGER",typeof(int))]
        [TestCase(DatabaseType.MySql, "BIT",typeof(bool))]
        [TestCase(DatabaseType.MySql, "SMALLINT(3)",typeof(short))]
        [TestCase(DatabaseType.MySql, "INT UNSIGNED",typeof(int))] //we don't distinguish between uint and int currently
        [TestCase(DatabaseType.MySql, "INT UNSIGNED ZEROFILL", typeof(int))]
        [TestCase(DatabaseType.MySql, "SMALLINT UNSIGNED",typeof(short))]
        [TestCase(DatabaseType.MySql, "SMALLINT ZEROFILL UNSIGNED",typeof(short))]
        [TestCase(DatabaseType.MySql, "LONGTEXT",typeof(string))]
        [TestCase(DatabaseType.MySql, "CHAR(10)",typeof(string))]
        [TestCase(DatabaseType.MySql, "TEXT",typeof(string))]
        [TestCase(DatabaseType.MySql, "BLOB",typeof(byte[]))]
        [TestCase(DatabaseType.MySql, "ENUM('fish','carrot')",typeof(string))]
        [TestCase(DatabaseType.MySql, "SET('fish','carrot')",typeof(string))]
        [TestCase(DatabaseType.MySql, "VARBINARY(10)",typeof(byte[]))]
        [TestCase(DatabaseType.MySql, "date",typeof(DateTime))]
        [TestCase(DatabaseType.MySql, "datetime",typeof(DateTime))]
        [TestCase(DatabaseType.MySql, "TIMESTAMP",typeof(DateTime))]
        [TestCase(DatabaseType.MySql, "TIME",typeof(TimeSpan))]
        [TestCase(DatabaseType.MySql, "nchar",typeof(string))]
        [TestCase(DatabaseType.MySql, "nvarchar(10)",typeof(string))]
        [TestCase(DatabaseType.MySql, "real",typeof(decimal))]
        [TestCase(DatabaseType.Oracle, "varchar2(10)",typeof(string))]
        public void TestIsKnownType(DatabaseType databaseType,string sqlType, Type expectedType)
        {
            RunKnownTypeTest(databaseType, sqlType, expectedType);
        }


        private void RunKnownTypeTest(DatabaseType type, string sqlType, Type expectedType)
        {
            var db = GetTestDatabase(type);
            var tt = db.Server.GetQuerySyntaxHelper().TypeTranslater;

            var tBefore = tt.GetCSharpTypeForSQLDBType(sqlType);

            //was the Type expected
            Assert.AreEqual(expectedType,tBefore);
            
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