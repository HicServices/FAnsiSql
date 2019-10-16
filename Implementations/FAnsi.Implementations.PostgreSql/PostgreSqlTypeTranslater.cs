using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Implementations.PostgreSql
{
    public class PostgreSqlTypeTranslater : TypeTranslater
    {
        public PostgreSqlTypeTranslater() : base(8000, 4000)
        {
        }

        public override string GetStringDataTypeWithUnlimitedWidth()
        {
            return "text";
        }

        public override string GetUnicodeStringDataTypeWithUnlimitedWidth()
        {
            return "text";
        }
    }
}