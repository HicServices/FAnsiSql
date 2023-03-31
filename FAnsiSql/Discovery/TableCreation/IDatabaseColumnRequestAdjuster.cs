using System.Collections.Generic;

namespace FAnsi.Discovery.TableCreation;

/// <summary>
/// Performs last minute changes on a set of columns that are about to be created.  This might include padding the maximum size of strings, using
/// varchar instead of int/DateTime etc.
/// </summary>
public interface IDatabaseColumnRequestAdjuster
{
    /// <summary>
    /// Implement to make last minute changes to the columns in the table being created
    /// </summary>
    /// <param name="columns"></param>
    void AdjustColumns(List<DatabaseColumnRequest> columns);
}