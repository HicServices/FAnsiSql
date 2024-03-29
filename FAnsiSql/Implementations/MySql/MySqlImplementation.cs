﻿using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

public sealed class MySqlImplementation() : Implementation<MySqlConnectionStringBuilder>(DatabaseType.MySql)
{
    public override IDiscoveredServerHelper GetServerHelper() => MySqlServerHelper.Instance;

    public override bool IsFor(DbConnection connection) => connection is MySqlConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => MySqlQuerySyntaxHelper.Instance;
}