using System;

namespace FAnsi.Exceptions;

/// <summary>
/// Thrown when a schema alter statement fails
/// </summary>
public sealed class AlterFailedException(string message, Exception inner) : Exception(message, inner);