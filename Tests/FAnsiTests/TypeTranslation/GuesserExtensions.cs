using FAnsi.Discovery.TypeTranslation;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation;

public static class GuesserExtensions
{
    public static string GetSqlDBType(this Guesser guesser, ITypeTranslater tt) => tt.GetSQLDBTypeForCSharpType(guesser.Guess);
}