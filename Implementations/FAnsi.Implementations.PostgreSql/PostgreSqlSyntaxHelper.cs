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

        public override string DatabaseTableSeparator { get; }
        public override int MaximumDatabaseLength { get; }
        public override int MaximumTableLength { get; }
        public override int MaximumColumnLength { get; }
        public override string EnsureWrappedImpl(string databaseOrTableName)
        {
            throw new NotImplementedException();
        }

        public override TopXResponse HowDoWeAchieveTopX(int x)
        {
            throw new NotImplementedException();
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