﻿namespace FAnsi.Discovery;

/// <summary>
/// Cross database type reference to a stored procedure (function) on a database.
/// </summary>
public sealed class DiscoveredStoredprocedure(string name)
{
    public string Name { get; set; } = name;
}