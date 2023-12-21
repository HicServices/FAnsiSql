using System.Data;
using FAnsi;
using FAnsi.Connections;
using NUnit.Framework;

namespace FAnsiTests;

internal sealed class ManagedConnectionTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_GetConnection_NotOpenAtStart(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var con = db.Server.GetConnection();

        //GetConnection should return an unopened connection
        Assert.That(con.State, Is.EqualTo(ConnectionState.Closed));
    }

    /// <summary>
    /// Tests that a managed connection is automatically opened and closed in dispose when there
    /// is no <see cref="IManagedTransaction"/> ongoing
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_GetManagedConnection_AutoOpenClose(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection con;
        using (con = db.Server.GetManagedConnection())
        {
            //GetManagedConnection should open itself
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
        }

        //finally should close it
        Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }


    /// <summary>
    /// Tests that a managed connection is automatically opened and closed in dispose when starting
    /// a new transaction
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_BeginNewTransactedConnection_AutoOpenClose(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection con;
        using (con = db.Server.BeginNewTransactedConnection())
        {
            //GetManagedConnection should open itself
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
        }

        //finally should close it
        Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }

    /// <summary>
    /// Tests that when passed an ongoing managed transaction the GetManagedConnection method
    /// reuses the exist <see cref="IDbConnection"/> and <see cref="IDbTransaction"/> and does
    /// not open or close them.
    ///
    /// <para>This is a design by the API to let us have using statements that either don't have a <see cref="IManagedTransaction"/> and handle
    /// opening and closing their own connections or do have a <see cref="IManagedTransaction"/> and ignore open/dispose step</para>
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_GetManagedConnection_OngoingTransaction(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection ongoingCon;
        //pretend that there is an ongoing transaction already
        using (ongoingCon = db.Server.BeginNewTransactedConnection())
        {
            var ongoingTrans = ongoingCon.ManagedTransaction;

            Assert.Multiple(() =>
            {
                //BeginNewTransactedConnection should open itself
                Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Open));
                Assert.That(ongoingTrans, Is.Not.Null);
            });

            //a managed connection with an ongoing transaction
            IManagedConnection con;
            using (con = db.Server.GetManagedConnection(ongoingTrans))
            {
                Assert.Multiple(() =>
                {
                    //still open
                    Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
                    Assert.That(con.Connection, Is.EqualTo(ongoingCon.Connection)); //same underlying connection
                });

            }
            //it should still be open after this finally block
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
        }

        //this is the using on the transaction this one should now close itself
        Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }


    /// <summary>
    /// Same as Test_GetManagedConnection_OngoingTransaction except we call <see cref="IManagedTransaction.CommitAndCloseConnection"/> or
    /// <see cref="IManagedTransaction.AbandonAndCloseConnection"/> instead of relying on the outermost using finally
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="commit">Whether to commit</param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypesWithBoolFlags))]
    public void Test_GetManagedConnection_OngoingTransaction_WithCommitRollback(DatabaseType dbType,bool commit)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection ongoingCon;
        //pretend that there is an ongoing transaction already
        using (ongoingCon = db.Server.BeginNewTransactedConnection())
        {
            var ongoingTrans = ongoingCon.ManagedTransaction;

            Assert.Multiple(() =>
            {
                //BeginNewTransactedConnection should open itself
                Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Open));
                Assert.That(ongoingTrans, Is.Not.Null);
            });

            //a managed connection with an ongoing transaction
            IManagedConnection con;
            using (con = db.Server.GetManagedConnection(ongoingTrans))
            {
                Assert.Multiple(() =>
                {
                    //still open
                    Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
                    Assert.That(con.Connection, Is.EqualTo(ongoingCon.Connection)); //same underlying connection
                });

            }
            //it should still be open after this finally block
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));

            if(commit)
                ongoingCon.ManagedTransaction.CommitAndCloseConnection();
            else
                ongoingCon.ManagedTransaction.AbandonAndCloseConnection();

            //that should really have closed it!
            Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Closed));
        }

        //this is the using on the transaction this one should now close itself
        Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }


    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_ManagedTransaction_MultipleCancel(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection ongoingCon;
        //pretend that there is an ongoing transaction already
        using (ongoingCon = db.Server.BeginNewTransactedConnection())
        {
            ongoingCon.ManagedTransaction.AbandonAndCloseConnection();
            ongoingCon.ManagedTransaction.AbandonAndCloseConnection();
            ongoingCon.ManagedTransaction.AbandonAndCloseConnection();
            ongoingCon.ManagedTransaction.CommitAndCloseConnection();
        }
    }

    /// <summary>
    /// Tests that a managed connection is automatically opened and closed in dispose when starting
    /// a new transaction
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_Clone_AutoOpenClose(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection con;
        using (con = db.Server.BeginNewTransactedConnection())
        {
            //GetManagedConnection should open itself
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));

            using (var clone = con.Clone())
            {
                clone.CloseOnDispose = false;
                Assert.Multiple(() =>
                {
                    //GetManagedConnection should open itself
                    Assert.That(clone.Connection.State, Is.EqualTo(ConnectionState.Open));

                    Assert.That(clone.Connection, Is.EqualTo(con.Connection));
                });
            }

            //GetManagedConnection should not have closed because we told the clone not to
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));

        } //now disposing the non clone

        //finally should close it
        Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }
}