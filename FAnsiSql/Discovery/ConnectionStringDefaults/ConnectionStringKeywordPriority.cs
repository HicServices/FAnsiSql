namespace FAnsi.Discovery.ConnectionStringDefaults;

/// <summary>
/// For use with <see cref="ConnectionStringKeywordAccumulator"/>, allows different parts of of your codebase to specify different values
/// of required keywords (e.g. AllowUserVariables) and overwrite one another based on priority.
/// </summary>
public enum ConnectionStringKeywordPriority
{
    /// <summary>
    /// Lowest priority e.g. settings defined in app config / global const parameters etc that you are happy to be overriden elsewhere
    /// </summary>
    SystemDefaultLow,
    /// <summary>
    /// Lowest priority e.g. settings defined in app config / global const parameters etc that you are happy to be overriden elsewhere
    /// </summary>
    SystemDefaultMedium,
    /// <summary>
    /// Lowest priority e.g. settings defined in app config / global const parameters etc that you are happy to be overriden elsewhere
    /// </summary>
    SystemDefaultHigh,

    /// <summary>
    /// User specified overrides for System Default settings.
    /// </summary>
    UserOverride,

    /// <summary>
    /// High level priority, the C# object being used is specifying a required keyword for it to operate correctly.  This overrides
    /// user settings and system defaults (but not <see cref="ApiRule"/>)
    /// </summary>
    ObjectOverride,

    /// <summary>
    /// Highest priority for keywords.  This is settings that cannot be unset/overriden by anyone else and are required
    /// for the API to work e.g.  AllowUserVariables in MySql
    /// </summary>
    ApiRule
}