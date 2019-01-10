using System;
using System.Globalization;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Extensions;

namespace FAnsi.Implementations.Oracle
{
    public class OracleTypeTranslater:TypeTranslater
    {
        private Regex AlsoFloatingPointRegex = new Regex("^(NUMBER)|(DEC)",RegexOptions.IgnoreCase);
        private Regex AlsoByteArrayRegex = new Regex("(BFILE)|(BLOB)|(RAW)|(ROWID)",RegexOptions.IgnoreCase);
        
        /// <summary>
        /// Oracle specific string types, these are all max length as returned by <see cref="GetLengthIfString"/>
        /// </summary>
        private Regex AlsoStringRegex = new Regex("^([N]?CLOB)|(LONG)", RegexOptions.IgnoreCase);

        public OracleTypeTranslater(): base(4000, 4000)
        {
            
        }
        protected override string GetStringDataTypeImpl(int maxExpectedStringWidth)
        {
            return "varchar2(" + maxExpectedStringWidth + ")";
        }

        public override string GetStringDataTypeWithUnlimitedWidth()
        {
            return "CLOB";
        }

        protected override string GetTimeDataType()
        {
            return "TIMESTAMP";
        }

        protected override string GetBoolDataType()
        {
            //See:
            //https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns
            return "char(1)";
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
    }
}
