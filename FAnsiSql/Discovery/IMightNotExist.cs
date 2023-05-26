using FAnsi.Connections;

namespace FAnsi.Discovery;

/// <summary>
/// Describes a database object that might not exist.  You can use methods that have keyword 'Expect' (e.g. DiscoveredServer.ExpectDatabase("bob")) to return
/// an object (DiscoveredDatabase in the example) without first checking that they exist.  Call IMightNotExist.Exists to confirm whether it still exists.
/// 
/// <para>The opposite approach is to use 'Discover' methods e.g. DiscoveredServer.DiscoverDatabases() to return all the DiscoveredDatabases found on the server.</para>
/// </summary>
public interface IMightNotExist
{
    /// <summary>
    /// Returns true if the object can be reached (e.g. connected to).
    /// </summary>
    /// <param name="transaction"></param>
    /// <returns></returns>
    bool Exists(IManagedTransaction transaction = null);
}