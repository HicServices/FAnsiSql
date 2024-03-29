﻿using System.Data;
using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;

namespace FAnsiTests.Query;

internal sealed class QuerySyntaxHelperDatabaseTests : DatabaseTests
{

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_HowDoWeAchieveMd5(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType,false);

        var sql = $"SELECT {db.Server.GetQuerySyntaxHelper().HowDoWeAchieveMd5("'fish'")}";


        //because Oracle :)
        if (dbType == DatabaseType.Oracle)
            sql += " FROM dual";

        using var con = db.Server.GetConnection();
        con.Open();

        var result = db.Server.GetCommand(sql, con).ExecuteScalar();

        Assert.That(result?.ToString(), Is.EqualTo("83E4A96AED96436C621B9809E258B309").IgnoreCase);
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_LenFunc(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType,false);

        using var dt = new DataTable();
        dt.Columns.Add("f");
        dt.Rows.Add("Troll Doll");

        var tbl = db.CreateTable("strlentesttable", dt);

        var len = tbl.GetQuerySyntaxHelper().GetScalarFunctionSql(MandatoryScalarFunctions.Len);

        using var con = tbl.Database.Server.GetConnection();
        con.Open();

        var sql = $"SELECT MAX({len}(f)) from {tbl.GetFullyQualifiedName()}";

        var cmd = tbl.Database.Server.GetCommand(sql, con);
        Assert.That(cmd.ExecuteScalar(), Is.EqualTo(10));
    }
}