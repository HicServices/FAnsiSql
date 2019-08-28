using System;
using System.Collections.ObjectModel;

namespace FAnsi.Discovery.TypeTranslation
{
    /// <summary>
    /// Describes a cross platform database field type you want created including maximum width for string based columns and precision/scale for decimals.
    /// 
    /// <para>See ITypeTranslater to see how a DatabaseTypeRequest is turned into the proprietary string e.g. A DatabaseTypeRequest with CSharpType = typeof(DateTime)
    /// is translated into 'datetime2' in Microsoft SQL Server but 'datetime' in MySql server.</para>
    /// </summary>
    public class DatabaseTypeRequest
    {
        /// <summary>
        /// Any input string of unknown Type will be assignable to one of the following C# data types.  The order denotes system wide which data types to try 
        /// converting the string into in order of preference.  For the implementation of this see <see cref="DataTypeComputer"/>.
        /// </summary>
        public static readonly ReadOnlyCollection<Type> PreferenceOrder = new ReadOnlyCollection<Type>(new Type[]
        {
            typeof(bool),
            typeof(int),
            typeof(decimal),

            typeof(TimeSpan),
            typeof(DateTime), //ironically Convert.ToDateTime likes int and floats as valid dates -- nuts
            
            typeof(string)
        });

        public Type CSharpType { get; set; }
        public int? MaxWidthForStrings { get; set; }
        public DecimalSize DecimalPlacesBeforeAndAfter { get; set; }

        /// <summary>
        /// Only applies when <see cref="CSharpType"/> is <see cref="string"/>.  True indicates that the column should be
        /// nvarchar instead of varchar.
        /// </summary>
        public bool Unicode { get;set;}

        public DatabaseTypeRequest(Type cSharpType, int? maxWidthForStrings = null,
            DecimalSize decimalPlacesBeforeAndAfter = null)
        {
            CSharpType = cSharpType;
            MaxWidthForStrings = maxWidthForStrings;
            DecimalPlacesBeforeAndAfter = decimalPlacesBeforeAndAfter;
        }

        #region Equality
        protected bool Equals(DatabaseTypeRequest other)
        {
            return Equals(CSharpType, other.CSharpType) && MaxWidthForStrings == other.MaxWidthForStrings && Equals(DecimalPlacesBeforeAndAfter, other.DecimalPlacesBeforeAndAfter);
        }

        public override bool Equals(object obj)
        {
            if (obj is null) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((DatabaseTypeRequest) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (CSharpType != null ? CSharpType.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ MaxWidthForStrings.GetHashCode();
                hashCode = (hashCode*397) ^ (DecimalPlacesBeforeAndAfter != null ? DecimalPlacesBeforeAndAfter.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(DatabaseTypeRequest left, DatabaseTypeRequest right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(DatabaseTypeRequest left, DatabaseTypeRequest right)
        {
            return !Equals(left, right);
        }
        #endregion

        public static DatabaseTypeRequest Max(DatabaseTypeRequest first, DatabaseTypeRequest second)
        {
            //if types differ
            if (PreferenceOrder.IndexOf(first.CSharpType) < PreferenceOrder.IndexOf(second.CSharpType))
            {
                second.Unicode = first.Unicode || second.Unicode;
                return second;
            }
            
            if (PreferenceOrder.IndexOf(first.CSharpType) > PreferenceOrder.IndexOf(second.CSharpType))
            {
                first.Unicode = first.Unicode || second.Unicode;
                return first;
            }
            
            if(!(first.CSharpType == second.CSharpType))
                throw new NotSupportedException(string.Format(FAnsiStrings.DatabaseTypeRequest_Max_Could_not_combine_Types___0___and___1___because_they_were_of_differing_Types_and_neither_Type_appeared_in_the_PreferenceOrder, first.CSharpType, second.CSharpType));

            //Types are the same, so max the sub elements (width, DecimalSize etc)

            int? newMaxWidthIfStrings = first.MaxWidthForStrings;

            //if first doesn't have a max string width
            if (newMaxWidthIfStrings == null)
                newMaxWidthIfStrings = second.MaxWidthForStrings; //use the second
            else if (second.MaxWidthForStrings != null)
                newMaxWidthIfStrings = Math.Max(newMaxWidthIfStrings.Value, second.MaxWidthForStrings.Value); //else use the max of the two

            //types are the same
            return new DatabaseTypeRequest(
                first.CSharpType,
                newMaxWidthIfStrings,
                DecimalSize.Combine(first.DecimalPlacesBeforeAndAfter, second.DecimalPlacesBeforeAndAfter)
                )
                {Unicode = first.Unicode || second.Unicode};

        }
    }
}
