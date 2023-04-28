using System;
using System.Data;

namespace FAnsi.Discovery;

/// <summary>
/// Thrown when a given column requested could not be matched in the destination table e.g. when inserting data in a <see cref="DataTable"/>
/// into a <see cref="DiscoveredTable"/> during a <see cref="BulkCopy"/> operation
/// </summary>
public class ColumnMappingException : Exception
{
    public ColumnMappingException(string msg):base(msg)
    {
            
    }

    public ColumnMappingException(string msg, Exception innerException):base(msg,innerException)
    {
            
    }
}