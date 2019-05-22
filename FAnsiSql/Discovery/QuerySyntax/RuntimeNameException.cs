using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Exceptions;
using System;
using System.Collections.Generic;
using System.Text;

namespace FAnsi.Discovery.QuerySyntax
{
    /// <summary>
    /// Exception thrown when <see cref="IQuerySyntaxHelper.GetRuntimeName"/> cannot determine the runtime name for a section of SQL
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
