using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.TableCreation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using TypeGuesser;

namespace FAnsiTests.Table;

internal sealed class BadNamesTests : DatabaseTests
{
    /// <summary>
    /// It would be a bad idea to name your column this but if you really wanted to...
    /// </summary>
    private static Tuple<string, string, string> GetBadNames(DatabaseType dbType)
    {
        var tblName = "Fi ; ][\"'`sh";
        var colName1 = "Da'   ][\",,;ve";
        var colName2 = "Frrrrr ##' ank";
        // JS 2023-05-11 Oracle doesn't allow " symbols even in quoted identifiers
        if (dbType == DatabaseType.Oracle)
        {
            tblName = tblName.Replace("\"", "");
            colName1 = colName1.Replace("\"", "");
            colName2 = colName2.Replace("\"", "");
        }
        return new Tuple<string, string, string>(tblName,colName1,colName2);
    }

    private DiscoveredTable SetupBadNamesTable(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var (badTableName,badColumnName,badColumnName2) = GetBadNames(dbType);
        return db.CreateTable(badTableName,
        [
            new DatabaseColumnRequest(badColumnName,new DatabaseTypeRequest(typeof(string),100)),
            new DatabaseColumnRequest(badColumnName2,new DatabaseTypeRequest(typeof(int)))
        ]);

    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_EnsureWrapped_EmptyExpressions(DatabaseType dbType)
    {
        var syntax = QuerySyntaxHelperFactory.Create(dbType);

        Assert.Multiple(() =>
        {
            Assert.That(syntax.EnsureWrapped(""), Is.EqualTo(""));
            Assert.That(syntax.EnsureWrapped(" "), Is.EqualTo(" "));
            Assert.That(syntax.EnsureWrapped("\t"), Is.EqualTo("\t"));
        });
        Assert.That(syntax.EnsureWrapped(null), Is.Null);

    }

    [Test]
    public void BadNames_EnsureWrapped()
    {
        var db = GetTestDatabase(DatabaseType.MicrosoftSQLServer);
        var tbl = db.ExpectTable("][nquisitor");

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists(), Is.False);
            Assert.That(tbl.GetWrappedName(), Is.EqualTo("[]][nquisitor]"));
        });
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BadNames_DiscoverColumns(DatabaseType dbType)
    {
        var tbl = SetupBadNamesTable(dbType);
        var cols = tbl.DiscoverColumns();
        Assert.That(cols, Has.Length.EqualTo(2));

        tbl.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BadNames_AlterType(DatabaseType dbType)
    {
        var tbl = SetupBadNamesTable(dbType);

        var (_, badColumnName, _) = GetBadNames(dbType);
        var col = tbl.DiscoverColumn(badColumnName);
        Assert.That(col.DataType?.GetLengthIfString(), Is.EqualTo(100));

        var varcharType = tbl.Database.Server.GetQuerySyntaxHelper().TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string),10));

        // Can we ALTER its datatype
        Assert.That(col.DataType.GetLengthIfString(), Is.EqualTo(100));
        col.DataType.AlterTypeTo(varcharType);
        Assert.That(col.DataType.GetLengthIfString(), Is.EqualTo(10));

        tbl.Drop();

    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void BadNames_TopXColumn(DatabaseType dbType,bool noNulls)
    {
        var (_, badColumnName, _) = GetBadNames(dbType);
        var tbl = SetupBadNamesTable(dbType);
        var col = tbl.DiscoverColumn(badColumnName);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
        tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
        tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,DBNull.Value } });

        Assert.That(tbl.GetRowCount(), Is.EqualTo(3));

        var topx = col.GetTopXSql(5,noNulls);

        var svr = tbl.Database.Server;
        using(var con = svr.GetConnection())
        {
            con.Open();
            var cmd = svr.GetCommand(topx,con);
            var r= cmd.ExecuteReader();

            Assert.That(r.Read());
            Assert.That(r.Read());

            Assert.That(r.Read(), Is.EqualTo(!noNulls));

            Assert.That(r.Read(), Is.False);
        }

        tbl.Drop();

    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BadNames_DropColumn(DatabaseType dbType)
    {
        var (_, badColumnName, _) = GetBadNames(dbType);
        var tbl = SetupBadNamesTable(dbType);

        Assert.That(tbl.DiscoverColumns(), Has.Length.EqualTo(2));

        var col = tbl.DiscoverColumn(badColumnName);

        tbl.DropColumn(col);

        Assert.That(tbl.DiscoverColumns(), Has.Length.EqualTo(1));

        tbl.Drop();
    }

    /////////// Table tests ///////////////////

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BadNames_TopXTable(DatabaseType dbType)
    {
        var (_, badColumnName, _) = GetBadNames(dbType);
        var tbl = SetupBadNamesTable(dbType);
        var col = tbl.DiscoverColumn(badColumnName);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
        tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,"ff" } });
        tbl.Insert(new Dictionary<DiscoveredColumn, object>{{col,DBNull.Value } });

        var topx = tbl.GetTopXSql(2);

        var svr = tbl.Database.Server;
        using(var con = svr.GetConnection())
        {
            con.Open();
            var cmd = svr.GetCommand(topx,con);
            var r= cmd.ExecuteReader();

            Assert.That(r.Read());
            Assert.That(r.Read());
            Assert.That(r.Read(), Is.False);
        }

        tbl.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BadNames_DiscoverRelationships(DatabaseType dbType)
    {
        var (badTableName, badColumnName, _) = GetBadNames(dbType);
        var db = GetTestDatabase(dbType);


        var tbl1 = db.CreateTable(badTableName,
        [
            new DatabaseColumnRequest(badColumnName,new DatabaseTypeRequest(typeof(string),100)){IsPrimaryKey = true },
            new DatabaseColumnRequest("Frrrrr ##' ank",new DatabaseTypeRequest(typeof(int)))
        ]);

        var pk = tbl1.DiscoverColumns().Single(static c=>c.IsPrimaryKey);
        DatabaseColumnRequest fk;

        var tbl2 = db.CreateTable(new CreateTableArgs(db, $"{badTableName}2",null)
        {
            ExplicitColumnDefinitions = [fk = new DatabaseColumnRequest($"{badColumnName}2",new DatabaseTypeRequest(typeof(string),100))],
            ForeignKeyPairs = new Dictionary<DatabaseColumnRequest, DiscoveredColumn> {{fk, pk} }
        });

        var r = tbl1.DiscoverRelationships().Single();

        Assert.Multiple(() =>
        {
            Assert.That(r.PrimaryKeyTable, Is.EqualTo(tbl1));
            Assert.That(r.ForeignKeyTable, Is.EqualTo(tbl2));

            Assert.That(r.Keys.Single().Key, Is.EqualTo(pk));
            Assert.That(r.Keys.Single().Value, Is.EqualTo(tbl2.DiscoverColumn($"{badColumnName}2")));
        });

        tbl2.Drop();
        tbl1.Drop();
    }

    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BadNames_BulkInsert(DatabaseType dbType)
    {
        var (_, badColumnName, badColumnName2) = GetBadNames(dbType);
        var tbl = SetupBadNamesTable(dbType);

        using var dt = new DataTable();
        dt.Columns.Add(badColumnName);
        dt.Columns.Add(badColumnName2);

        dt.Rows.Add ("fff", 5);

        using(var insert = tbl.BeginBulkInsert())
        {
            insert.Upload(dt);
        }

        Assert.That(dt.Rows, Has.Count.EqualTo(1));
        tbl.Drop();
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void BadNames_Rename(DatabaseType dbType)
    {
        var (badTableName, _, _) = GetBadNames(dbType);
        var tbl = SetupBadNamesTable(dbType);

        var nameBefore = tbl.GetFullyQualifiedName();

        tbl.Rename(badTableName.Replace('F','A'));

        Assert.That(tbl.GetFullyQualifiedName(), Is.Not.EqualTo(nameBefore));

        tbl.Drop();
    }
}