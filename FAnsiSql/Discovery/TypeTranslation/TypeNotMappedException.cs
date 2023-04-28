using System;

namespace FAnsi.Discovery.TypeTranslation;

/// <summary>
/// Thrown when a given C# Type is not mapped to a known DBMS proprietary type by FAnsi or vice versa.
/// </summary>
public class TypeNotMappedException : Exception
{
    public TypeNotMappedException(string msg):base(msg)
    {   
    }

    public TypeNotMappedException(string msg, Exception innerException) : base(msg, innerException)
    {

    }
}