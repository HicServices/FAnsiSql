using System.Data;
using FAnsi;
using FAnsi.Connections;
using NUnit.Framework;

namespace FAnsiTests;

internal class ManagedConnectionTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_GetConnection_NotOpenAtStart(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var con = db.Server.GetConnection();

        //GetConnection should return an unopened connection
        Assert.AreEqual(ConnectionState.Closed,con.State);
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
            Assert.AreEqual(ConnectionState.Open,con.Connection.State);
        }

        //finally should close it
        Assert.AreEqual(ConnectionState.Closed,con.Connection.State);
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
            Assert.AreEqual(ConnectionState.Open,con.Connection.State);
        }

        //finally should close it
        Assert.AreEqual(ConnectionState.Closed,con.Connection.State);
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

            //BeginNewTransactedConnection should open itself
            Assert.AreEqual(ConnectionState.Open,ongoingCon.Connection.State);
            Assert.IsNotNull(ongoingTrans);

            //a managed connection with an ongoing transaction
            IManagedConnection con;
            using (con = db.Server.GetManagedConnection(ongoingTrans))
            {
                //still open
                Assert.AreEqual(ConnectionState.Open,con.Connection.State);
                Assert.IsTrue(con.Connection == ongoingCon.Connection); //same underlying connection

            }
            //it should still be open after this finally block
            Assert.AreEqual(ConnectionState.Open,con.Connection.State);
        }

        //this is the using on the transaction this one should now close itself
        Assert.AreEqual(ConnectionState.Closed,ongoingCon.Connection.State);
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

            //BeginNewTransactedConnection should open itself
            Assert.AreEqual(ConnectionState.Open,ongoingCon.Connection.State);
            Assert.IsNotNull(ongoingTrans);

            //a managed connection with an ongoing transaction
            IManagedConnection con;
            using (con = db.Server.GetManagedConnection(ongoingTrans))
            {
                //still open
                Assert.AreEqual(ConnectionState.Open,con.Connection.State);
                Assert.IsTrue(con.Connection == ongoingCon.Connection); //same underlying connection

            }
            //it should still be open after this finally block
            Assert.AreEqual(ConnectionState.Open,con.Connection.State);

            if(commit)
                ongoingCon.ManagedTransaction.CommitAndCloseConnection();
            else
                ongoingCon.ManagedTransaction.AbandonAndCloseConnection();

            //that should really have closed it!
            Assert.AreEqual(ConnectionState.Closed,ongoingCon.Connection.State);
        }

        //this is the using on the transaction this one should now close itself
        Assert.AreEqual(ConnectionState.Closed,ongoingCon.Connection.State);
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
            Assert.AreEqual(ConnectionState.Open,con.Connection.State);

            using (var clone = con.Clone())
            {
                clone.CloseOnDispose = false;
                //GetManagedConnection should open itself
                Assert.AreEqual(ConnectionState.Open,clone.Connection.State);

                Assert.IsTrue(clone.Connection == con.Connection);
            }

            //GetManagedConnection should not have closed because we told the clone not to
            Assert.AreEqual(ConnectionState.Open,con.Connection.State);

        } //now disposing the non clone

        //finally should close it
        Assert.AreEqual(ConnectionState.Closed,con.Connection.State);
    }
}