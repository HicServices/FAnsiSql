using System;

namespace FAnsi.Discovery.Exceptions
{
    /// <summary>
    /// Thrown when a schema alter statement fails
    /// </summary>
    public class AlterFailedException : Exception
    {
        public AlterFailedException(string message, Exception inner)
            : base(message, inner)
        {
            
        }
    }
}