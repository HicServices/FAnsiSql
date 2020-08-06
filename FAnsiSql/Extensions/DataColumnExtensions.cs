using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using FAnsi.Discovery;

namespace FAnsi.Extensions
{
    public static class DataColumnExtensions
    {
        /// <summary>
        /// Extended Property you can set in (<see cref="DataColumn.ExtendedProperties"/>) to true to suppress Parsing and
        /// Type Guessing for specific string columns.  Use <see cref="SetDoNotReType"/> to set this
        /// </summary>
        public const string DoNotReTypeExtendedProperty = "DoNotReType";

        /// <summary>
        /// Sets an <see cref="DataColumn.ExtendedProperties"/> on <paramref name="dc"/> to indicate to FAnsi
        /// that it should not attempt to change the Type of the column e.g. when creating a table based on the
        /// <see cref="DataTable"/>.
        ///
        /// <para>Method has no effect on columns where the <see cref="DataColumn.DataType"/> is not <see cref="string"/></para>
        /// </summary>
        /// <param name="dc"></param>
        /// <param name="value">True to prevent retyping, false to allow it</param>
        public static void SetDoNotReType(this DataColumn dc, bool value)
        {
            if(!dc.ExtendedProperties.ContainsKey(DoNotReTypeExtendedProperty))
                dc.ExtendedProperties.Add(DoNotReTypeExtendedProperty, value);
            else
                dc.ExtendedProperties[DoNotReTypeExtendedProperty] = value;
        }

        /// <summary>
        /// Returns true if the <see cref="DataColumn.ExtendedProperties"/> of the <see cref="DataColumn"/> lists
        /// true for <see cref="DoNotReTypeExtendedProperty"/> and the <see cref="DataColumn.DataType"/> is string
        /// </summary>
        /// <param name="dc"></param>
        /// <returns></returns>
        public static bool GetDoNotReType(this DataColumn dc)
        {
            return     
                dc.DataType == typeof(string) &&
                dc.ExtendedProperties.ContainsKey(DoNotReTypeExtendedProperty) &&
                dc.ExtendedProperties[DoNotReTypeExtendedProperty] is bool b
                && b;
        }

    }
}
