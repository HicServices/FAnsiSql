using System;
using System.Collections.Generic;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System.Linq;

namespace FAnsiTests.Aggregation
{
    class AggregationTests:DatabaseTests
    {
        protected readonly Dictionary<DatabaseType, DiscoveredTable> _testTables = new Dictionary<DatabaseType, DiscoveredTable>();
            
        [OneTimeSetUp]
        public void Setup()
        {
            try
            {
                using (DataTable dt = new DataTable())
                {
                    dt.TableName = "AggregateDataBasedTests";

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

                    dt.Rows.Add("2001-01-01", "E&, %a' mp;E", "37");
                    dt.Rows.Add("2002-01-01", "E&, %a' mp;E", "41");
                    dt.Rows.Add("2005-01-01", "E&, %a' mp;E", "59");  //note there are no records in 2004 it is important for axis tests (axis involves you having to build a calendar table)

                    dt.Rows.Add(null, "G", "47");
                    dt.Rows.Add("2001-01-01", "G", "53");

            
                    foreach (KeyValuePair<DatabaseType, string> kvp in TestConnectionStrings)
                    {
                        try
                        {
                            var db = GetTestDatabase(kvp.Key);
                            var tbl = db.CreateTable("AggregateDataBasedTests", dt);
                            _testTables.Add(kvp.Key,tbl);

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Could not setup test database for DatabaseType " + kvp.Key);
                            Console.WriteLine(e);

                        }
                        
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        protected void AssertHasRow(DataTable dt, params object[] cells)
        {
            Assert.IsTrue(dt.Rows.Cast<DataRow>().Any(r=>IsMatch(r,cells)),"Did not find expected row:" + string.Join("|",cells));
        }

        /// <summary>
        /// Confirms that the first x cells of <paramref name="r"/> match the contents of <paramref name="cells"/>
        /// </summary>
        /// <param name="r"></param>
        /// <param name="cells"></param>
        /// <returns></returns>
        protected bool IsMatch(DataRow r, object[] cells)
        {
            for(int i = 0 ; i<cells.Length ;i++)
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
                    }
            
                if (!a.Equals(b))
                    return false;
            }                
            
            return true;
        }


        protected void ConsoleWriteTable(DataTable dt)
        {
            Console.WriteLine("--- DebugTable(" + dt.TableName + ") ---");
            int zeilen = dt.Rows.Count;
            int spalten = dt.Columns.Count;

            // Header
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                string s = dt.Columns[i].ToString();
                Console.Write(String.Format("{0,-20} | ", s));
            }
            Console.Write(Environment.NewLine);
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                Console.Write("---------------------|-");
            }
            Console.Write(Environment.NewLine);

            // Data
            for (int i = 0; i < zeilen; i++)
            {
                DataRow row = dt.Rows[i];
                //Console.WriteLine("{0} {1} ", row[0], row[1]);
                for (int j = 0; j < spalten; j++)
                {
                    string s = row[j].ToString();
                    if (s.Length > 20) s = s.Substring(0, 17) + "...";
                    Console.Write(String.Format("{0,-20} | ", s));
                }
                Console.Write(Environment.NewLine);
            }
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                Console.Write("---------------------|-");
            }
            Console.Write(Environment.NewLine);
        }

        protected DiscoveredTable GetTestTable(DatabaseType type)
        {
            if (!_testTables.ContainsKey(type))
                Assert.Inconclusive("No connection string found for Test database type {0}", type);
            
            return _testTables[type];
        }
            
    }
}
