using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using System.Collections.Generic;
using System.Linq;

namespace FAnsi.Discovery.QuerySyntax.Aggregation
{
    public abstract class AggregateHelper:IAggregateHelper
    {
        public abstract string BuildAggregate(List<CustomLine> queryLines, IQueryAxis axisIfAny, bool pivot);

        /// <summary>
        /// Provides useful bits of SQL you need to build an axis only aggregate
        /// </summary>
        /// <param name="syntaxHelper">Your DBMS specific syntax helper</param>
        /// <param name="lines">The lines you were given in <see cref="BuildAggregate"/></param>
        /// <param name="countSelectLine">The single aggregate function line e.g. "count(distinct chi) as Fish,"</param>
        /// <param name="countSqlWithoutAlias">The portion of <paramref name="countSelectLine"/> which excludes the alias e.g. "count(distinct chi)"</param>
        /// <param name="countAlias">The portion of <paramref name="countSelectLine"/> which is the alias e.g. "Fish" (or null if no AS is specified)</param>
        /// <param name="axisColumn">The single line of SELECT SQL which is the Axis join column e.g. "[MyDb]..[mytbl].[AdmissionDate] as Admt,"</param>
        /// <param name="axisColumnWithoutAlias">The portion of <paramref name="axisColumn"/> which excludes the alias e.g. "[MyDb]..[mytbl].[AdmissionDate]"</param>
        /// <param name="axisColumnAlias">The portion of <paramref name="axisColumn"/> which is the alias e.g. "Admt" (or "joinDt" if no AS is specified)</param>
        protected void GetAggregateAxisBits(IQuerySyntaxHelper syntaxHelper, List<CustomLine> lines,
            out CustomLine countSelectLine,
            out string countSqlWithoutAlias,
            out string countAlias,
            out CustomLine axisColumn,
            out string axisColumnWithoutAlias,
            out string axisColumnAlias)
        {
            countSelectLine = lines.Single(l => l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.CountFunction);
            syntaxHelper.SplitLineIntoSelectSQLAndAlias(countSelectLine.Text, out countSqlWithoutAlias, out countAlias);
            
            //Deal with the axis dimension which is currently `mydb`.`mytbl`.`mycol` and needs to become YEAR(`mydb`.`mytbl`.`mycol`) As joinDt 
            axisColumn = lines.Single(l => l.LocationToInsert == QueryComponent.QueryTimeColumn && l.Role == CustomLineRole.Axis);

            syntaxHelper.SplitLineIntoSelectSQLAndAlias(axisColumn.Text, out axisColumnWithoutAlias, out axisColumnAlias);

            if (string.IsNullOrWhiteSpace(axisColumnAlias))
                axisColumnAlias = "joinDt";
        }


        protected void WrapAxisColumnWithDatePartFunction(CustomLine axisColumn, List<CustomLine> lines, IQueryAxis axis, string axisColumnWithoutAlias, string axisColumnAlias)
        {
            var axisGroupBy = lines.Single(l => l.LocationToInsert == QueryComponent.GroupBy && l.Role == CustomLineRole.Axis);

            var axisColumnEndedWithComma = axisColumn.Text.EndsWith(",");
            axisColumn.Text = GetDatePartOfColumn(axis.AxisIncrement, axisColumnWithoutAlias) + " AS " + axisColumnAlias + (axisColumnEndedWithComma ? "," : "");

            var groupByEndedWithComma = axisGroupBy.Text.EndsWith(",");
            axisGroupBy.Text = GetDatePartOfColumn(axis.AxisIncrement, axisColumnWithoutAlias) + (groupByEndedWithComma ? "," : "");
        }

        public abstract string GetDatePartOfColumn(AxisIncrement increment, string columnSql);
    }
}