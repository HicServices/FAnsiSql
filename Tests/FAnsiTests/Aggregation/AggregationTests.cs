using System;
using System.Collections.Generic;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System.Linq;

namespace FAnsiTests.Aggregation;

internal class AggregationTests:DatabaseTests
{
    private readonly Dictionary<DatabaseType, DiscoveredTable> _easyTables = [];
    private readonly Dictionary<DatabaseType, DiscoveredTable> _hardTables = [];

    [OneTimeSetUp]
    public void Setup()
    {
        SetupDatabaseTable(true, "AggregateDataBasedTestsEasy");

        SetupDatabaseTable(false, "AggregateDataBasedTestsHard");
    }

    private void SetupDatabaseTable(bool easy, string name)
    {
        try
        {
            using var dt = new DataTable();
            dt.TableName = name;

            dt.Columns.Add("EventDate");
            dt.Columns.Add("Category");
            dt.Columns.Add("NumberInTrouble");

            dt.Rows.Add("2001-01-01", "T", "7");
            dt.Rows.Add("2001-01-02", "T", "11");
            dt.Rows.Add("2001-01-01", "T", "49");

            dt.Rows.Add("2002-02-01", "T", "13");
            dt.Rows.Add("2002-03-02", "T", "17");
            dt.Rows.Add("2003-01-01", "T", "19");
            dt.Rows.Add("2003-04-02", "T", "23");


            dt.Rows.Add("2002-01-01", "F", "29");
            dt.Rows.Add("2002-01-01", "F", "31");

            if (!easy)
            {
                dt.Rows.Add("2001-01-01", "E&, %a' mp;E", "37");
                dt.Rows.Add("2002-01-01", "E&, %a' mp;E", "41");
                dt.Rows.Add("2005-01-01", "E&, %a' mp;E", "59");  //note there are no records in 2004 it is important for axis tests (axis involves you having to build a calendar table)
            }


            dt.Rows.Add(null, "G", "47");
            dt.Rows.Add("2001-01-01", "G", "53");


            foreach (var key in TestConnectionStrings.Keys)
                try
                {
                    var db = GetTestDatabase(key);
                    var tbl = db.CreateTable("AggregateDataBasedTests", dt);

                    var dic = easy ? _easyTables : _hardTables;
                    dic.Add(key, tbl);

                }
                catch (Exception e)
                {
                    TestContext.WriteLine($"Could not setup test database for DatabaseType {key}");
                    TestContext.WriteLine(e);

                }
        }
        catch (Exception e)
        {
            TestContext.WriteLine(e);
            throw;
        }
    }

    protected static void AssertHasRow(DataTable dt, params object?[] cells)
    {
        Assert.That(dt.Rows.Cast<DataRow>().Any(r => IsMatch(r, cells)),$"Did not find expected row:{string.Join("|", cells)}");
    }

    /// <summary>
    /// Confirms that the first x cells of <paramref name="r"/> match the contents of <paramref name="cells"/>
    /// </summary>
    /// <param name="r"></param>
    /// <param name="cells"></param>
    /// <returns></returns>
    private static bool IsMatch(DataRow r, object?[] cells)
    {
        for (var i = 0; i < cells.Length; i++)
        {
            var a = r[i];
            var b = cells[i] ?? DBNull.Value; //null means dbnull

            var aType = a.GetType();
            var bType = b.GetType();

            //could be dealing with int / long mismatch etc
            if (aType != bType)
                try
                {
                    b = Convert.ChangeType(b, aType);
                }
                catch (Exception)
                {
                    //they are not a match because they are not the same type and cannot be converted
                    return false;
                }

            if (!a.Equals(b))
                return false;
        }

        return true;
    }


    protected static void ConsoleWriteTable(DataTable _)
    {
        /*
        TestContext.WriteLine($"--- DebugTable({dt.TableName}) ---");
        var zeilen = dt.Rows.Count;
        var spalten = dt.Columns.Count;

        // Header
        for (var i = 0; i < dt.Columns.Count; i++)
        {
            var s = dt.Columns[i].ToString();
            TestContext.Write($"{s,-20} | ");
        }
        TestContext.Write(Environment.NewLine);
        for (var i = 0; i < dt.Columns.Count; i++)
        {
            TestContext.Write("---------------------|-");
        }
        TestContext.Write(Environment.NewLine);

        // Data
        for (var i = 0; i < zeilen; i++)
        {
            var row = dt.Rows[i];
            //TestContext.WriteLine("{0} {1} ", row[0], row[1]);
            for (var j = 0; j < spalten; j++)
            {
                var s = row[j].ToString();
                if (s?.Length > 20) s = $"{s[..17]}...";
                TestContext.Write($"{s,-20} | ");
            }
            TestContext.Write(Environment.NewLine);
        }
        for (var i = 0; i < dt.Columns.Count; i++)
        {
            TestContext.Write("---------------------|-");
        }
        TestContext.Write(Environment.NewLine);
    */
    }

    protected DiscoveredTable GetTestTable(DatabaseType type, bool easy = false)
    {
        var dic = easy ? _easyTables : _hardTables;

        if (!dic.ContainsKey(type))
            Assert.Inconclusive($"No connection string found for Test database type {type}");

        return dic[type];
    }

}