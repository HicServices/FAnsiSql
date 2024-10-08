﻿using System;
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

    public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database) => throw new NotImplementedException();

    protected override string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype, IQuerySyntaxHelper syntaxHelper)
    {
        if (col.IsAutoIncrement)
            return $"{col.ColumnName} INTEGER {syntaxHelper.GetAutoIncrementKeywordIfAny()}";
        if (datatype.Equals("bigint", StringComparison.OrdinalIgnoreCase))
            return $"{col.ColumnName} NUMBER(19,0)";

        return base.GetCreateTableSqlLineForColumn(col, datatype, syntaxHelper);
    }

    public override DirectoryInfo Detach(DiscoveredDatabase database) => throw new NotImplementedException();

    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        throw new NotImplementedException();
    }

    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction? transaction = null)
    {
        //find all the tables
        using (var cmd = new OracleCommand($"SELECT table_name FROM all_tables where owner='{database}'", (OracleConnection)connection))
        {
            cmd.Transaction = transaction as OracleTransaction;

            var r = cmd.ExecuteReader();

            while (r.Read())
                //skip invalid table names
                if (querySyntaxHelper.IsValidTableName((string)r["table_name"], out _))
                    yield return new DiscoveredTable(parent, (string)r["table_name"], querySyntaxHelper);
        }

        //find all the views
        if (!includeViews) yield break;

        using (var cmd = new OracleCommand($"SELECT view_name FROM all_views where owner='{database}'",
                   (OracleConnection)connection))
        {
            cmd.Transaction = transaction as OracleTransaction;
            var r = cmd.ExecuteReader();

            while (r.Read())
            {
                var name = (string)r["view_name"];
                if (querySyntaxHelper.IsValidTableName(name, out _))
                    yield return new DiscoveredTable(parent, name, querySyntaxHelper, null,
                        TableType.View);
            }
        }
    }

    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction? transaction = null) =>
        Array.Empty<DiscoveredTableValuedFunction>();

    public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database) => [];

    protected override Guesser GetGuesser(DatabaseTypeRequest request) =>
        new(request)
            {ExtraLengthPerNonAsciiCharacter = OracleTypeTranslater.ExtraLengthPerNonAsciiCharacter};

    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {
        //Oracle doesn't really have schemas especially since a User is a Database
    }

    protected override Guesser GetGuesser(DataColumn column) => new() {ExtraLengthPerNonAsciiCharacter = OracleTypeTranslater.ExtraLengthPerNonAsciiCharacter};
}