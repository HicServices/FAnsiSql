using System;
using System.Collections.Generic;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.MySql.Aggregation;
using FAnsi.Implementations.MySql.Update;
using FAnsi.Naming;

namespace FAnsi.Implementations.MySql
{
    public class MySqlQuerySyntaxHelper : QuerySyntaxHelper
    {
        public override int MaximumDatabaseLength => 64;
        public override int MaximumTableLength => 64;
        public override int MaximumColumnLength => 64;

        public MySqlQuerySyntaxHelper() : base(new MySqlTypeTranslater(), new MySqlAggregateHelper(),new MySqlUpdateHelper(),DatabaseType.MySql)//no specific type translation required
        {
        }

        public override bool SupportsEmbeddedParameters()
        {
            return true;
        }

        public override string EnsureWrappedImpl(string databaseOrTableName)
        {
            return "`" + GetRuntimeNameWithDoubledBackticks(databaseOrTableName) + "`";
        }

        /// <summary>
        /// Returns the runtime name of the string with all backticks escaped (but resulting string is not wrapped in backticks itself)
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        private string GetRuntimeNameWithDoubledBackticks(string s)
        {
            return GetRuntimeName(s)?.Replace("`","``");
        }
        public override string EnsureFullyQualified(string databaseName, string schema, string tableName)
        {
            //if there is no schema address it as db..table (which is the same as db.dbo.table in Microsoft SQL Server)
            if (!string.IsNullOrWhiteSpace(schema))
                throw new NotSupportedException("Schema (e.g. .dbo. not supported by MySql)");

            return  EnsureWrapped(databaseName) + DatabaseTableSeparator + EnsureWrapped(tableName);
        }

        public override TopXResponse HowDoWeAchieveTopX(int x)
        {
            return new TopXResponse("LIMIT " + x,QueryComponent.Postfix);
        }

        public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType)
        {
            //MySql doesn't require parameter declaration you just start using it like javascript
            return "/*" + proposedNewParameterName + "*/";
        }

        public override string Escape(string sql)
        {
            //https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
            
            sql = sql.Replace("\\", "\\\\"); //first of all swap current \ for \\ (don't do this later because we are going to inject a bunch of that stuff!).

            sql = sql.Replace("'", "\\'"); //swap ' for \'

            sql = sql.Replace("\"", "\\\""); //swap " for \"
            sql = sql.Replace("\r\n", "\\n"); //swap newline whitespace with \r for \n
            sql = sql.Replace("\n", "\\n"); //swap newline whitespace for \n
            sql = sql.Replace("\t", "\\t"); //swap tab whitespace for \t

            //only apply in pattern matching use cases (rare?) otherwise they break it! you will have to handle this yourself if you have that situation
            //sql = sql.Replace("%", "\\%"); //swap % for \%
            //sql = sql.Replace("_", "\\_"); //swap _ for \_
            
            return sql;
        }

        public override string GetScalarFunctionSql(MandatoryScalarFunctions function)
        {
            
            switch (function)
            {
                case MandatoryScalarFunctions.GetTodaysDate:  //this works at least as of 5.7.19
                    return "now()";
                case MandatoryScalarFunctions.GetGuid:  //using this as defaults in columns requires MySql 8 (2018)
                    return "(uuid())"; 
                case MandatoryScalarFunctions.Len:
                    return "LENGTH";
                default:
                    throw new ArgumentOutOfRangeException("function");
            }
        }

        public override string GetAutoIncrementKeywordIfAny()
        {
            return "AUTO_INCREMENT";
        }

        public override Dictionary<string, string> GetSQLFunctionsDictionary()
        {
            return new Dictionary<string, string>()
            {
                {"left", "LEFT ( string , length)"},
                {"right", "RIGHT ( string , length )"},
                {"upper", "UPPER ( string )"},
                {"substring", "SUBSTR ( str ,start , length ) "},
                {"dateadd", "DATE_ADD (date, INTERVAL value unit)"},
                {"datediff", "DATEDIFF ( date1 , date2)  "},
                {"getdate", "now()"},
                {"now", "now()"},
                {"cast", "CAST ( value AS type )"},
                {"convert", "CONVERT ( value, type ) "},
                {"case", "CASE WHEN x=y THEN 'something' WHEN x=z THEN 'something2' ELSE 'something3' END"}
            };
        }

        public override string HowDoWeAchieveMd5(string selectSql)
        {
            return "md5(" + selectSql + ")";
        }
    }
}