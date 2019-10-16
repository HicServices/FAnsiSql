using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.PostgreSql.Aggregation
{
    public class PostgreSqlAggregateHelper : AggregateHelper
    {
        protected override IQuerySyntaxHelper GetQuerySyntaxHelper()
        {
            return new PostgreSqlSyntaxHelper();
        }

        protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
        {
            throw new System.NotImplementedException();
        }

        protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
        {
            throw new System.NotImplementedException();
        }

        protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
        {
            throw new System.NotImplementedException();
        }

        public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
        {
            throw new System.NotImplementedException();
        }
    }
}