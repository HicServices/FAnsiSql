using System;

namespace FAnsi.Discovery.QuerySyntax;

/// <summary>
/// thrown when there is a problem with the name of an object (e.g. a column / table) or when one could not be calculated from a piece of SQL
/// </summary>
/// <remarks>
/// Creates a new instance of the Exception with the given <paramref name="message"/>
/// </remarks>
/// <param name="message"></param>
public sealed class RuntimeNameException(string message) : Exception(message);