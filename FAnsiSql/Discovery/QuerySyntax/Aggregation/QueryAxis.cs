namespace FAnsi.Discovery.QuerySyntax.Aggregation
{
    /// <inheritdoc/>
    public class QueryAxis : IQueryAxis
    {
        /// <inheritdoc/>
        public string EndDate { get; set; }
        /// <inheritdoc/>
        public string StartDate { get; set; }
        /// <inheritdoc/>
        public AxisIncrement AxisIncrement { get; set; }
    }
}