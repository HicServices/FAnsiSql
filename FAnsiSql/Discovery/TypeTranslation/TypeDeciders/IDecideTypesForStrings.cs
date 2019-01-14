using System;
using System.Collections.Generic;

namespace FAnsi.Discovery.TypeTranslation.TypeDeciders
{
    /// <summary>
    /// Class responsible for deciding whether a given string representation is likely to be for a given C# Type e.g. 
    /// "2001-01-01" is likley to be a date.
    /// 
    /// <para>Each IDecideTypesForStrings should be for a single Type although different sizes is allowed e.g. Int16, Int32, Int64</para>
    /// 
    /// <para>Implementations should be as mutually exclusive with as possible.  Look also at <see cref="DatabaseTypeRequest.PreferenceOrder"/> and <see cref="DataTypeComputer"/></para>
    /// </summary>
    public interface IDecideTypesForStrings
    {
        /// <summary>
        /// Determines how the decider interacts with other deciders e.g. IntTypeDecider can fallback to DecimalTypeDecider because you can store ints in decimal columns.
        /// </summary>
        TypeCompatibilityGroup CompatibilityGroup { get; }

        /// <summary>
        /// List of input Types which the decider will evaluate true
        /// </summary>
        HashSet<Type> TypesSupported { get; }

        /// <summary>
        /// Returns true if the <paramref name="candidateString"/> is convertable and safely modelled as one of the <see cref="TypesSupported"/>.
        /// </summary>
        /// <param name="candidateString">a string containing a value of unknown type (e.g. "fish" or "1")</param>
        /// <param name="sizeRecord">The current size estimate of floating point numbers (or null if not appropriate).  This will be modified by the method 
        /// if appropriate to the data passed</param>
        /// <returns>True if the <paramref name="candidateString"/> is a valid value for the <see cref="TypesSupported"/> by the decider</returns>
        bool IsAcceptableAsType(string candidateString,DecimalSize sizeRecord);

        /// <summary>
        /// Converts the provided <paramref name="value"/> to an object of the Type modelled by this <see cref="IDecideTypesForStrings"/>.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        object Parse(string value);
    }
}
