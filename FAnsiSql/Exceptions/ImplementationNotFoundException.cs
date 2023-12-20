using System;

namespace FAnsi.Exceptions;

public sealed class ImplementationNotFoundException(string message) : Exception(message);