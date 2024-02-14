using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

public sealed class All
{
    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on all DBMS
    /// </summary>
    public static readonly DatabaseType[] DatabaseTypes = [
        DatabaseType.MicrosoftSQLServer,
        DatabaseType.MySql,
        DatabaseType.Oracle,
        DatabaseType.PostgreSql
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on all DBMS
    /// with both permutations of true/false.  Matches exhaustively method signature (DatabaseType,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithBoolFlags = [
        new object[] {DatabaseType.MicrosoftSQLServer,true},
        new object[] {DatabaseType.MySql,true},
        new object[] {DatabaseType.Oracle,true},
        new object[] {DatabaseType.PostgreSql,true},
        new object[] {DatabaseType.MicrosoftSQLServer,false},
        new object[] {DatabaseType.MySql,false},
        new object[] {DatabaseType.Oracle,false},
        new object[] {DatabaseType.PostgreSql,false}
    ];


    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on all DBMS
    /// with all permutations of true/false for 2 args.  Matches exhaustively method signature (DatabaseType,bool,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [
        new object[] {DatabaseType.MicrosoftSQLServer,true,true},
        new object[] {DatabaseType.MicrosoftSQLServer,true,false},
        new object[] {DatabaseType.MicrosoftSQLServer,false,true},
        new object[] {DatabaseType.MicrosoftSQLServer,false,false},

        new object[] {DatabaseType.MySql,true,true},
        new object[] {DatabaseType.MySql,true,false},
        new object[] {DatabaseType.MySql,false,true},
        new object[] {DatabaseType.MySql,false,false},


        new object[] {DatabaseType.Oracle,true,true},
        new object[] {DatabaseType.Oracle,true,false},
        new object[] {DatabaseType.Oracle,false,true},
        new object[] {DatabaseType.Oracle,false,false},

        new object[] {DatabaseType.PostgreSql,true,true},
        new object[] {DatabaseType.PostgreSql,true,false},
        new object[] {DatabaseType.PostgreSql,false,true},
        new object[] {DatabaseType.PostgreSql,false,false}
    ];
}