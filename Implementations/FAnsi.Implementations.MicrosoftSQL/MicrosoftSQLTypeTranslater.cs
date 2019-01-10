using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;

namespace Fansi.Implementations.MicrosoftSQL
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

        protected override bool IsByteArray(string sqlType)
        {
            return base.IsByteArray(sqlType) || AlsoBinaryRegex.IsMatch(sqlType);
        }
    }
}