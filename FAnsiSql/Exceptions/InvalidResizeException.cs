using System;

namespace FAnsi.Exceptions
{
    /// <summary>
    /// Exception thrown when you ask to resize a column to a size that is smaller than it's current size
    /// </summary>
    public class InvalidResizeException : Exception
    {
        public InvalidResizeException(string s):base(s)
        {
            
        }
    }
}