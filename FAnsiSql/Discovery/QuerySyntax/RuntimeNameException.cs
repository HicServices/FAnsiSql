using System;

namespace FAnsi.Discovery.QuerySyntax
{
    /// <summary>
    /// thrown when there is a problem with the name of an object (e.g. a column / table) or when one could not be calculated from a piece of SQL
    /// </summary>
    public class RuntimeNameException:Exception
    {
        /// <summary>
        /// Creates a new instance of the Exception with the given <paramref name="message"/> and <paramref name="innerException"/>
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public RuntimeNameException(string message, Exception innerException):base(message,innerException)
        {
                
        }

        /// <summary>
        /// Creates a new instance of the Exception with the given <paramref name="message"/>
        /// </summary>
        /// <param name="message"></param>
        public RuntimeNameException(string message):base(message)
        {
                
        }
    }
}
