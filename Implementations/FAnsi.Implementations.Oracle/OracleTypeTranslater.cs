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

        protected override string GetDateDateTimeDataType()
        {
            return "DATE";
        }
    }
}
