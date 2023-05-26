using System.Linq;
using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal class TableValuedFunctionTests:DatabaseTests
{
    [TestCase("dbo")]
    [TestCase("Omg")]
    public void Test_DropTableValuedFunction(string schema)
    {
        var db = GetTestDatabase(DatabaseType.MicrosoftSQLServer);

        using (var con = db.Server.GetConnection())
        {
            con.Open();

            //create the schema if it doesn't exist yet
            if(schema != null)
                db.Server.GetCommand($@"
IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{schema}' ) 
EXEC('CREATE SCHEMA  {schema}')", con).ExecuteNonQuery();


            var sql = $@"CREATE FUNCTION {schema}.MyAwesomeFunction
(	
	-- Add the parameters for the function here
	@startNumber int ,
	@stopNumber int,
	@name varchar(50)
)
RETURNS
@ReturnTable TABLE 
(
	-- Add the column definitions for the TABLE variable here
	Number int, 
	Name varchar(50)
)
AS
BEGIN
	-- Fill the table variable with the rows for your result set
	DECLARE @i int;
	set @i = @startNumber

	while(@i < @stopNumber)
		begin
		INSERT INTO @ReturnTable(Name,Number) VALUES (@name,@i);
		set @i = @i + 1;
		end

	RETURN 
END";
            db.Server.GetCommand(sql,con).ExecuteNonQuery();
        }

        var tvf = db.DiscoverTableValuedFunctions().Single();
        var p = tvf.DiscoverParameters().First();
        Assert.AreEqual("@startNumber",p.ParameterName);
        Assert.AreEqual("int",p.DataType.SQLType);
        Assert.AreEqual(schema,tvf.Schema??"dbo");

        StringAssert.EndsWith(".MyAwesomeFunction(@startNumber,@stopNumber,@name)",tvf.GetFullyQualifiedName());

        Assert.IsTrue(tvf.Exists());

        tvf.Drop();

        Assert.IsFalse(tvf.Exists());

            
    }
}