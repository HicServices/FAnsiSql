using System;
using FAnsi.Discovery.TypeTranslation;

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
            if (sqlType.Equals("CLOB", StringComparison.InvariantCultureIgnoreCase))
                return true;

            return base.IsString(sqlType);
        }

        public override int GetLengthIfString(string sqlType)
        {
            if (sqlType.Equals("CLOB", StringComparison.InvariantCultureIgnoreCase))
                return int.MaxValue;

            return base.GetLengthIfString(sqlType);
        }

        protected override bool IsTime(string sqlType)
        {
            return sqlType.StartsWith("timestamp", StringComparison.CurrentCultureIgnoreCase) || base.IsTime(sqlType);
        }

        protected override string GetDateDateTimeDataType()
        {
            return "DATE";
        }
    }
}
