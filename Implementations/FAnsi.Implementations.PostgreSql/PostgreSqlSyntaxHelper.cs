using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.PostgreSql.Aggregation;
using FAnsi.Implementations.PostgreSql.Update;
using TypeGuesser;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlSyntaxHelper : QuerySyntaxHelper
    {
        public PostgreSqlSyntaxHelper() : base(new PostgreSqlTypeTranslater(), new PostgreSqlAggregateHelper(), new PostgreSqlUpdateHelper(), DatabaseType.PostgreSql)
        {
        }

        public override int MaximumDatabaseLength => 63;
        public override int MaximumTableLength => 63;
        public override int MaximumColumnLength => 63;
        public override string EnsureWrappedImpl(string databaseOrTableName)
        {
            return '"' + GetRuntimeName(databaseOrTableName) + '"';
        }

        public override TopXResponse HowDoWeAchieveTopX(int x)
        {
            return new TopXResponse("fetch first " + x + " rows only", QueryComponent.Postfix);
        }

        public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType)
        {
            throw new NotImplementedException();
        }

        public override string GetScalarFunctionSql(MandatoryScalarFunctions function)
        {
            throw new NotImplementedException();
        }

        public override string GetAutoIncrementKeywordIfAny()
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, string> GetSQLFunctionsDictionary()
        {
            throw new NotImplementedException();
        }

        public override string HowDoWeAchieveMd5(string selectSql)
        {
            throw new NotImplementedException();
        }
    }
}