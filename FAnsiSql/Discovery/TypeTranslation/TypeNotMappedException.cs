using System;

namespace FAnsi.Discovery.TypeTranslation;

/// <summary>
/// Thrown when a given C# Type is not mapped to a known DBMS proprietary type by FAnsi or vice versa.
/// </summary>
public sealed class TypeNotMappedException(string msg) : Exception(msg);