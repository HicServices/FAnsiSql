using System.Collections.Generic;

namespace FAnsi.Discovery.QuerySyntax.Aggregation
{
    /// <summary>
    /// Cross Database Type class for turning a collection of arbitrary sql lines (CustomLine) into a Group by query.  The query can include an axis calendar 
    /// table and can include a dynamic pivot.  See AggregateDataBasedTests for expected inputs/outputs.
    /// 
    /// <para>Because building a dynamic pivot / calendar table for a group by is so different in each DatabaseType the input is basically just a collection of strings
    /// with roles and it is up to the implementation to resolve them into something that will run.  The basic case (no axis and no pivot) should be achievable
    /// just by concatenating the CustomLines.</para>
    /// </summary>
    public interface IAggregateHelper
    {
        /// <summary>
        /// Returns an SQL statement that can be run on the DBMS being implemented that runs a GROUP BY query.  Optionally this query must join to a calendar table
        /// defined by <paramref name="axisIfAny"/>.  Optionally this query must include a dynamic pivot column.
        /// </summary>
        /// <param name="queryLines"></param>
        /// <param name="axisIfAny"></param>
        /// <returns></returns>
        string BuildAggregate(List<CustomLine> queryLines, IQueryAxis axisIfAny);
    }
}
