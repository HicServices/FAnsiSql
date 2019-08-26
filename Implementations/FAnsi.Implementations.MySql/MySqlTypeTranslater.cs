using System;
using System.Globalization;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Extensions;

namespace FAnsi.Implementations.MySql
{
    public class MySqlTypeTranslater : TypeTranslater
    {
        //yup thats right!, long is string (MEDIUMTEXT)
        //https://dev.mysql.com/doc/refman/8.0/en/other-vendor-data-types.html
        private readonly Regex AlsoBitRegex = new Regex(@"tinyint\(1\)",RegexOptions.IgnoreCase);
        private readonly Regex AlsoStringRegex = new Regex("(long)|(enum)|(set)|(text)|(mediumtext)",RegexOptions.IgnoreCase);
        private readonly Regex AlsoFloatingPoint = new Regex("^(dec)|(fixed)",RegexOptions.IgnoreCase);

        public MySqlTypeTranslater() : base(4000, 4000)
        {
            //match bigint and bigint(20) etc
            ByteRegex = new Regex(@"^(tinyint)|(int1)",RegexOptions.IgnoreCase);
            SmallIntRegex = new Regex(@"^(smallint)|(int2)", RegexOptions.IgnoreCase);
            IntRegex = new Regex(@"^(int)|(mediumint)|(middleint)|(int3)|(int4)",RegexOptions.IgnoreCase);
            LongRegex = new Regex(@"^(bigint)|(int8)", RegexOptions.IgnoreCase);
            DateRegex = new Regex(@"(date)|(timestamp)",RegexOptions.IgnoreCase);
        }

        public override string GetStringDataTypeWithUnlimitedWidth()
        {
            return "text";
        }

        public override string GetUnicodeStringDataTypeWithUnlimitedWidth()
        {
            return "text";
        }

        protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth)
        {
            return "varchar(" + maxExpectedStringWidth + ")";
        }

        protected override bool IsInt(string sqlType)
        {
            //not an int
            if (sqlType.StartsWith("int8", StringComparison.CurrentCultureIgnoreCase))
                return false;

            return base.IsInt(sqlType);
        }

        protected override bool IsString(string sqlType)
        {
            if (sqlType.Contains("binary",CompareOptions.IgnoreCase))
                return false;

            return base.IsString(sqlType) || AlsoStringRegex.IsMatch(sqlType);
        }

        protected override bool IsFloatingPoint(string sqlType)
        {
            return base.IsFloatingPoint(sqlType) || AlsoFloatingPoint.IsMatch(sqlType);
        }

        protected override bool IsBit(string sqlType)
        {
            return base.IsBit(sqlType) || AlsoBitRegex.IsMatch(sqlType);
        }
    }
}