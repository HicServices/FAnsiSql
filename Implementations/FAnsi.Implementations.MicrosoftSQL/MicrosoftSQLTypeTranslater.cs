using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Implementations.MicrosoftSQL
{
    public class MicrosoftSQLTypeTranslater : TypeTranslater
    {
        protected Regex AlsoBinaryRegex = new Regex("(image)|(timestamp)|(rowversion)",RegexOptions.IgnoreCase);


        public MicrosoftSQLTypeTranslater() : base(8000, 4000)
        {
        }

        protected override string GetDateDateTimeDataType()
        {
            return "datetime2";
        }
        
        public override string GetStringDataTypeWithUnlimitedWidth()
        {
            return "varchar(max)";
        }

        public override string GetUnicodeStringDataTypeWithUnlimitedWidth()
        {
            return "nvarchar(max)";
        }

        protected override bool IsByteArray(string sqlType)
        {
            return base.IsByteArray(sqlType) || AlsoBinaryRegex.IsMatch(sqlType);
        }
    }
}