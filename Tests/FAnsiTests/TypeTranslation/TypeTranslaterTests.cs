using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Exceptions;
using FAnsi.Implementation;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation;

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
    private readonly Dictionary<DatabaseType,ITypeTranslater> _translaters = [];

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
    [TestCase(DatabaseType.PostgreSql, "varchar(10)")]
    public void Test_CSharpToDbType_String10(DatabaseType type,string expectedType)
    {
        var cSharpType = new DatabaseTypeRequest(typeof (string), 10);

        Assert.That(_translaters[type].GetSQLDBTypeForCSharpType(cSharpType), Is.EqualTo(expectedType));}

    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(max)")]
    [TestCase(DatabaseType.MySql, "longtext")]
    [TestCase(DatabaseType.Oracle, "CLOB")]
    [TestCase(DatabaseType.PostgreSql, "text")]
    public void Test_CSharpToDbType_StringMax(DatabaseType type,string expectedType)
    {
        var cSharpType = new DatabaseTypeRequest(typeof(string), 10000000);

        Assert.Multiple(() =>
        {
            //Does a request for a max length string give the expected data type?
            Assert.That(_translaters[type].GetSQLDBTypeForCSharpType(cSharpType), Is.EqualTo(expectedType));

            //Does the TypeTranslater know that this datatype has no limit on characters?
            Assert.That(_translaters[type].GetLengthIfString(expectedType), Is.EqualTo(int.MaxValue));

            //And does the TypeTranslater know that this datatype is string
            Assert.That(_translaters[type].GetCSharpTypeForSQLDBType(expectedType), Is.EqualTo(typeof(string)));
        });
    }

    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(max)",false)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "nvarchar(max)",true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "text",false)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "ntext",true)]
    [TestCase(DatabaseType.MySql, "longtext",false)]
    [TestCase(DatabaseType.Oracle, "CLOB", false)]
    [TestCase(DatabaseType.PostgreSql, "text", false)]
    public void Test_GetLengthIfString_VarcharMaxCols(DatabaseType type, string datatype, bool expectUnicode)
    {
        Assert.That(_translaters[type].GetLengthIfString(datatype), Is.EqualTo(int.MaxValue));
        var dbType = _translaters[type].GetDataTypeRequestForSQLDBType(datatype);

        Assert.Multiple(() =>
        {
            Assert.That(dbType.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(dbType.Width, Is.EqualTo(int.MaxValue));
            Assert.That(dbType.Unicode, Is.EqualTo(expectUnicode));
        });
    }

    [TestCase(DatabaseType.MicrosoftSQLServer,"bigint",typeof(long))]
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
    [TestCase(DatabaseType.MySql, "INT(1)", typeof(int))] //Confusing but these are actually display names https://stackoverflow.com/questions/11563830/what-does-int1-stand-for-in-mysql
    [TestCase(DatabaseType.MySql, "INT(2)",typeof(int))]
    [TestCase(DatabaseType.MySql, "INT(3)",typeof(int))]
    [TestCase(DatabaseType.MySql, "INT(4)",typeof(int))]
    [TestCase(DatabaseType.MySql, "INT(8)",typeof(int))]
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
    [TestCase(DatabaseType.Oracle, "CHAR(10)", typeof(string))]
    [TestCase(DatabaseType.Oracle, "CHAR", typeof(string))]
    [TestCase(DatabaseType.Oracle, "nchar", typeof(string))]
    [TestCase(DatabaseType.Oracle, "nvarchar2(1)", typeof(string))]
    [TestCase(DatabaseType.Oracle, "clob", typeof(string))]
    [TestCase(DatabaseType.Oracle, "nclob", typeof(string))]
    [TestCase(DatabaseType.Oracle, "long", typeof(string))]//yes in Oracle LONG is text (https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm)
    [TestCase(DatabaseType.Oracle, "NUMBER", typeof(decimal))]
    [TestCase(DatabaseType.Oracle, "date", typeof(DateTime))]
    [TestCase(DatabaseType.Oracle, "BLOB", typeof(byte[]))]
    [TestCase(DatabaseType.Oracle, "BFILE", typeof(byte[]))]
    [TestCase(DatabaseType.Oracle, "RAW(100)", typeof(byte[]))]
    [TestCase(DatabaseType.Oracle, "LONG RAW", typeof(byte[]))]
    [TestCase(DatabaseType.Oracle, "ROWID", typeof(byte[]))]
    [TestCase(DatabaseType.Oracle, "CHARACTER", typeof(string))]
    [TestCase(DatabaseType.Oracle, "FLOAT", typeof(decimal))]
    [TestCase(DatabaseType.Oracle, "FLOAT(5)", typeof(decimal))]
    [TestCase(DatabaseType.Oracle, "REAL", typeof(decimal))]
    [TestCase(DatabaseType.Oracle, "DOUBLE PRECISION", typeof(decimal))]
    [TestCase(DatabaseType.Oracle, "CHARACTER VARYING(10)", typeof(string))]
    [TestCase(DatabaseType.Oracle, "CHAR VARYING(10)", typeof(string))]
    [TestCase(DatabaseType.Oracle, "LONG VARCHAR", typeof(string))]

    //[TestCase(DatabaseType.Oracle, "DECIMAL", typeof(decimal))] //GetBasicTypeFromOracleType makes this look like dcimal going in but Int32 comming out
    //[TestCase(DatabaseType.Oracle, "DEC", typeof(decimal))]

    [TestCase(DatabaseType.Oracle, "DEC(3,2)", typeof(decimal))]
    [TestCase(DatabaseType.Oracle, "DEC(*,3)", typeof(decimal))]

    //These types are all converted to Number(38) by Oracle : https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm (See ANSI/ISO, DB2, and SQL/DS Datatypes )
    [TestCase(DatabaseType.Oracle, "INTEGER", typeof(int))]
    [TestCase(DatabaseType.Oracle, "INT", typeof(int))]
    [TestCase(DatabaseType.Oracle, "SMALLINT", typeof(int))] //yup, see the link above
    public void TestIsKnownType(DatabaseType databaseType,string sqlType, Type expectedType)
    {
        RunKnownTypeTest(databaseType, sqlType, expectedType);
    }

    private void RunKnownTypeTest(DatabaseType type, string sqlType, Type expectedType)
    {
        //Get test database
        var db = GetTestDatabase(type);
        var tt = db.Server.GetQuerySyntaxHelper().TypeTranslater;

        //Create it in database (crashes here if it's an invalid datatype according to DBMS)
        var tbl = db.CreateTable("TTT", new[] { new DatabaseColumnRequest("MyCol", sqlType) });

        try
        {
            //Find the column on the created table and fetch it
            var col = tbl.DiscoverColumns().Single();

            //What type does FAnsi think this is?
            var tBefore = tt.TryGetCSharpTypeForSQLDBType(sqlType);
            Assert.That(tBefore, Is.Not.Null, "We asked to create a '{0}', DBMS created a '{1}'.  FAnsi didn't recognise '{0}' as a supported Type",sqlType,col.DataType.SQLType);

            //Does FAnsi understand the datatype that was actually created on the server (sometimes you specify something and it is an
            //alias for something else e.g. Oracle creates 'varchar2' when you ask for 'CHAR VARYING'
            var Guesser = col.GetGuesser();
            Assert.That(Guesser.Guess.CSharpType, Is.Not.Null);
            var tAfter = Guesser.Guess.CSharpType;

            Assert.Multiple(() =>
            {
                //was the Type REQUESTED correct according to the test case expectation
                Assert.That(tBefore, Is.EqualTo(expectedType), $"We asked to create a '{sqlType}', DBMS created a '{col.DataType.SQLType}'.  FAnsi decided that '{sqlType}' is '{tBefore}' and that '{col.DataType.SQLType}' is '{tAfter}'");

                //Was the Type CREATED matching the REQUESTED type (as far as FAnsi is concerned)
                Assert.That(tAfter, Is.EqualTo(tBefore), $"We asked to create a '{sqlType}', DBMS created a '{col.DataType.SQLType}'.  FAnsi decided that '{sqlType}' is '{tBefore}' and that '{col.DataType.SQLType}' is '{tAfter}'");
            });

            if (!string.Equals(col.DataType.SQLType,sqlType,StringComparison.CurrentCultureIgnoreCase))
                TestContext.WriteLine("{0} created a '{1}' when asked to create a '{2}'", type,
                    col.DataType.SQLType, sqlType);

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
    [TestCase(DatabaseType.Oracle, "MLSLABEL")]
    public void TestNotSupportedTypes(DatabaseType type, string sqlType)
    {
        Assert.That(_translaters[type].IsSupportedSQLDBType(sqlType), Is.False);
    }
}