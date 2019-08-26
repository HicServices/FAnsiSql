using System;
using System.Globalization;
using System.Text.RegularExpressions;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Extensions;

namespace FAnsi.Implementations.Oracle
{
    public class OracleTypeTranslater:TypeTranslater
    {
        private readonly Regex AlsoFloatingPointRegex = new Regex("^(NUMBER)|(DEC)",RegexOptions.IgnoreCase);
        private readonly Regex AlsoByteArrayRegex = new Regex("(BFILE)|(BLOB)|(RAW)|(ROWID)",RegexOptions.IgnoreCase);
        
        public const int ExtraLengthPerNonAsciiCharacter = 3;

        /// <summary>
        /// Oracle specific string types, these are all max length as returned by <see cref="GetLengthIfString"/>
        /// </summary>
        private readonly Regex AlsoStringRegex = new Regex("^([N]?CLOB)|(LONG)", RegexOptions.IgnoreCase);

        public OracleTypeTranslater(): base(4000, 4000)
        {
            DateRegex = new Regex("(date)|(timestamp)", RegexOptions.IgnoreCase);
        }
        protected override string GetStringDataTypeImpl(int maxExpectedStringWidth)
        {
            return "varchar2(" + maxExpectedStringWidth + ")";
        }

        protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth)
        {
            return "nvarchar2(" + maxExpectedStringWidth + ")";
        }

        public override string GetStringDataTypeWithUnlimitedWidth()
        {
            return "CLOB";
        }

        public override string GetUnicodeStringDataTypeWithUnlimitedWidth()
        {
            return "NCLOB";
        }

        protected override string GetTimeDataType()
        {
            return "TIMESTAMP";
        }

        /// <summary>
        /// <para>Returns char(5).  Oracle doesn't have a bit character type.  You can only approximate it with char(1) or number(1) and an independent named CHECK constraint
        /// For our purposes we will have to just use varchar2(5) to store "True" or "False" and "1" and "0" etc</para>
        /// 
        /// <para>See https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns</para>
        /// </summary>
        /// <returns></returns>
        protected override string GetBoolDataType()
        {
            return "varchar2(5)";
        }

        /// <summary>
        /// <para>Returns False.  Oracle doesn't have a bit character type.  You can only approximate it with char(1) or number(1) and an independent named CHECK constraint
        /// For our purposes we will have to just use varchar2(5) to store "True" or "False" and "1" and "0" etc</para>
        /// 
        /// <para>See https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns</para>
        /// </summary>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        protected override bool IsBit(string sqlType)
        {
            return false;
        }

        protected override bool IsString(string sqlType)
        {
            if (sqlType.Contains("RAW",CompareOptions.IgnoreCase))
                return false;

            return base.IsString(sqlType) || AlsoStringRegex.IsMatch(sqlType);
        }

        protected override bool IsFloatingPoint(string sqlType)
        {
            return base.IsFloatingPoint(sqlType) || AlsoFloatingPointRegex.IsMatch(sqlType);
        }

        public override int GetLengthIfString(string sqlType)
        {
            if (AlsoStringRegex.IsMatch(sqlType))
                return int.MaxValue;

            return base.GetLengthIfString(sqlType);
        }

        protected override bool IsSmallInt(string sqlType)
        {
            //yup you ask for one of these, you will get a NUMBER(38) https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm
            if(sqlType.StartsWith("SMALLINT", StringComparison.CurrentCultureIgnoreCase))
                return false;

            return base.IsSmallInt(sqlType);
        }

        protected override bool IsInt(string sqlType)
        {
            //yup you ask for one of these, you will get a NUMBER(38) https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm
            if (sqlType.StartsWith("SMALLINT", StringComparison.CurrentCultureIgnoreCase))
                return true;

            return base.IsInt(sqlType);
        }

        protected override bool IsByteArray(string sqlType)
        {
            return base.IsByteArray(sqlType) || AlsoByteArrayRegex.IsMatch(sqlType);
        }
        
        protected override string GetDateDateTimeDataType()
        {
            return "DATE";
        }

        protected override DataTypeComputer GetDataTypeComputer(Type currentEstimatedType, DecimalSize decimalSize, int lengthIfString, bool unicode)
        {
            return new DataTypeComputer(currentEstimatedType, decimalSize, lengthIfString, ExtraLengthPerNonAsciiCharacter){UseUnicode = unicode};
        }
    }
}
