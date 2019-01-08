using System;
using System.Collections.Generic;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.Oracle.Aggregation;
using FAnsi.Implementations.Oracle.Update;

namespace FAnsi.Implementations.Oracle
{
    public class OracleQuerySyntaxHelper : QuerySyntaxHelper
    {
        public OracleQuerySyntaxHelper() : base(new OracleTypeTranslater(), new OracleAggregateHelper(),new OracleUpdateHelper(),DatabaseType.Oracle)//no custom translater
        {
        }

        public override char ParameterSymbol
        {
            get { return ':'; }
        }

        public override string GetRuntimeName(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return s;

            //upper it because oracle loves uppercase stuff
            string toReturn =  s.Substring(s.LastIndexOf(".") + 1).Trim('"').ToUpper();

            //truncate it to 30 maximum because oracle cant count higher than 30
            return toReturn.Length > 30 ? toReturn.Substring(0, 30) : toReturn;

        }

        public override string EnsureWrappedImpl(string databaseOrTableName)
        {
            return '"' + GetRuntimeName(databaseOrTableName) + '"';
        }

        public override TopXResponse HowDoWeAchieveTopX(int x)
        {
            return new TopXResponse("ROWNUM <= " + x, QueryComponent.WHERE);
        }

        public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType)
        {
            throw new System.NotImplementedException();
        }

        public override string GetScalarFunctionSql(MandatoryScalarFunctions function)
        {
            switch (function)
            {
                case MandatoryScalarFunctions.GetTodaysDate:
                    return "CURRENT_TIMESTAMP";
                    case MandatoryScalarFunctions.GetGuid:
                    return "SYS_GUID()";
                default:
                    throw new ArgumentOutOfRangeException("function");
            }
        }

        /// <summary>
        /// Always returns null for Oracle since this is handled by <see cref="OracleDatabaseHelper.GetCreateTableSql"/>
        /// </summary>
        /// <returns></returns>
        public override string GetAutoIncrementKeywordIfAny()
        {
            //this is handled in 
            return null;
        }

        public override Dictionary<string, string> GetSQLFunctionsDictionary()
        {
            return new Dictionary<string, string>();
        }

        public override string HowDoWeAchieveMd5(string selectSql)
        {
            throw new NotImplementedException();
        }

        protected override object FormatTimespanForDbParameter(TimeSpan timeSpan)
        {
            //Value must be a DateTime even if DBParameter is of Type DbType.Time
            return Convert.ToDateTime(timeSpan.ToString());
        }

        public override string DatabaseTableSeparator
        {
            get { return "."; }
        }
    }
}