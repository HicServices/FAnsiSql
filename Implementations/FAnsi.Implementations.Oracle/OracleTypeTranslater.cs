using System;
using System.Globalization;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Extensions;

namespace FAnsi.Implementations.Oracle
{
    public class OracleTypeTranslater:TypeTranslater
    {
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
            if (sqlType.Contains("CLOB", CompareOptions.IgnoreCase))
                return true;

            //LONG but not LONG RAW!
            if (sqlType.StartsWith("LONG", StringComparison.CurrentCultureIgnoreCase) && !sqlType.Contains("RAW", CompareOptions.IgnoreCase))
                return true;
            
            return base.IsString(sqlType);
        }

        protected override bool IsFloatingPoint(string sqlType)
        {
            if (sqlType.StartsWith("NUMBER", StringComparison.CurrentCultureIgnoreCase))
                return true;

            if (sqlType.StartsWith("DEC", StringComparison.CurrentCultureIgnoreCase))
                return true;

            return base.IsFloatingPoint(sqlType);
        }

        public override int GetLengthIfString(string sqlType)
        {
            if (sqlType.Contains("CLOB", CompareOptions.IgnoreCase))
                return int.MaxValue;

            return base.GetLengthIfString(sqlType);
        }

        protected override bool IsTime(string sqlType)
        {
            return sqlType.StartsWith("timestamp", StringComparison.CurrentCultureIgnoreCase) || base.IsTime(sqlType);
        }

        protected override bool IsSmallInt(string sqlType)
        {
            //yup you ask for one of these, you will get a NUMBER(38) https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm
            if(sqlType.StartsWith("SMALLINT", StringComparison.CurrentCultureIgnoreCase))
                return false;

            return base.IsSmallInt(sqlType);
        }

        protected override bool IsByteArray(string sqlType)
        {
            if(sqlType.StartsWith("BFILE",StringComparison.CurrentCultureIgnoreCase))
                return true;
            if (sqlType.StartsWith("BLOB", StringComparison.CurrentCultureIgnoreCase))
                return true;
            if (sqlType.Contains("RAW", CompareOptions.IgnoreCase))
                return true;
            if (sqlType.Contains("ROWID", CompareOptions.IgnoreCase))
                return true;

            return base.IsByteArray(sqlType);
        }

        protected override string GetDateDateTimeDataType()
        {
            return "DATE";
        }
    }
}
