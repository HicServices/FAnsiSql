﻿using System;
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
            var answer = base.GetRuntimeName(s);

            if (string.IsNullOrWhiteSpace(answer))
                return s;
            
            //upper it because oracle loves uppercase stuff
            string toReturn =  answer.Trim('"').ToUpper();

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
        /// Works in Oracle 12c+ only https://oracle-base.com/articles/12c/identity-columns-in-oracle-12cr1
        /// </summary>
        /// <returns></returns>
        public override string GetAutoIncrementKeywordIfAny()
        {
            //this is handled in 
            return " GENERATED ALWAYS AS IDENTITY";
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