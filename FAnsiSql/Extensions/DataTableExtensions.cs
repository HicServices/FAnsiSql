using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace FAnsi.Extensions
{
    public static class DataTableExtensions
    {
        /// <summary>
        /// Sets an <see cref="DataColumn.ExtendedProperties"/> on all string columns of <paramref name="dt"/> to indicate to FAnsi
        /// that it should not attempt to change the Type of the string columns e.g. when creating a table.
        ///
        /// <para>Method has no effect on columns where the <see cref="DataColumn.DataType"/> is not <see cref="string"/></para>
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="value">True to prevent datatype changes, false to allow</param>
        public static void SetDoNotReType(this DataTable dt, bool value)
        {
            foreach (DataColumn dc in dt.Columns)
                dc.SetDoNotReType(value);
        }
    }
}
