using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using Oracle.ManagedDataAccess.Client;
using TypeGuesser;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleDatabaseHelper : DiscoveredDatabaseHelper
{
    public static readonly OracleDatabaseHelper Instance=new();
    private OracleDatabaseHelper(){}
    public override IDiscoveredTableHelper GetTableHelper() => OracleTableHelper.Instance;

    public override void DropDatabase(DiscoveredDatabase database)
    {
        using var con = (OracleConnection)database.Server.GetConnection();
        con.Open();
        using var cmd = new OracleCommand($"DROP USER \"{database.GetRuntimeName()}\" CASCADE ",con);
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
    {
        throw new NotImplementedException();
    }

    protected override string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype, IQuerySyntaxHelper syntaxHelper)
    {
        if (col.IsAutoIncrement)
            return $"{col.ColumnName} INTEGER {syntaxHelper.GetAutoIncrementKeywordIfAny()}";

        return base.GetCreateTableSqlLineForColumn(col, datatype, syntaxHelper);
    }

    public override DirectoryInfo Detach(DiscoveredDatabase database)
    {
        throw new NotImplementedException();
    }

    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        throw new NotImplementedException();
    }

    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction transaction = null)
    {
        var tables = new List<DiscoveredTable>();
            
        //find all the tables
        using(var cmd = new OracleCommand($"SELECT table_name FROM all_tables where owner='{database}'", (OracleConnection) connection))
        {
            cmd.Transaction = transaction as OracleTransaction;

            var r = cmd.ExecuteReader();

            while (r.Read())
                //skip invalid table names
                if(querySyntaxHelper.IsValidTableName((string)r["table_name"],out _))
                    tables.Add(new DiscoveredTable(parent,r["table_name"].ToString(),querySyntaxHelper));
        }
            
        //find all the views
        if(includeViews)
        {
            using var cmd = new OracleCommand($"SELECT view_name FROM all_views where owner='{database}'", (OracleConnection) connection);
            cmd.Transaction = transaction as OracleTransaction;
            var r = cmd.ExecuteReader();
                
            while (r.Read())
                if(querySyntaxHelper.IsValidTableName((string)r["view_name"],out _))
                    tables.Add(new DiscoveredTable(parent,r["view_name"].ToString(),querySyntaxHelper,null,TableType.View));
        }


        return tables.ToArray();
    }

    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction transaction = null)
    {
        return Array.Empty<DiscoveredTableValuedFunction>();
    }
        
    public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database)
    {
        return Array.Empty<DiscoveredStoredprocedure>();
    }

    protected override Guesser GetGuesser(DatabaseTypeRequest request)
    {
        return new Guesser(request)
            {ExtraLengthPerNonAsciiCharacter = OracleTypeTranslater.ExtraLengthPerNonAsciiCharacter};
    }

    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {
        //Oracle doesn't really have schemas especially since a User is a Database
    }

    protected override Guesser GetGuesser(DataColumn column)
    {
        return new Guesser {ExtraLengthPerNonAsciiCharacter = OracleTypeTranslater.ExtraLengthPerNonAsciiCharacter};
    }


}