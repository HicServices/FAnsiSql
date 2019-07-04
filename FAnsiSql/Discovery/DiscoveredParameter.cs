namespace FAnsi.Discovery
{
    /// <summary>
    /// Cross database type reference to a Parameter (e.g. of a Table valued function / stored proceedure).
    /// </summary>
    public class DiscoveredParameter
    {
        /// <summary>
        /// SQL name of parameter e.g. @bob for Sql Server
        /// </summary>
        public string ParameterName { get; set; }
        public DiscoveredParameter(string parameterName)
        {
            ParameterName = parameterName;
        }

        /// <summary>
        /// The <see cref="DiscoveredDataType"/> the parameter is declared as e.g. varchar(10)
        /// </summary>
        public DiscoveredDataType DataType { get; set; }
    }
}
