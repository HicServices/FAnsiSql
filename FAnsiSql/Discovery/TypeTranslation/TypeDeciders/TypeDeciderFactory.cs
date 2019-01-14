using System;
using System.Collections.Generic;

namespace FAnsi.Discovery.TypeTranslation.TypeDeciders
{
    /// <summary>
    /// Hosts all <see cref="IDecideTypesForStrings"/> and indexes them by <see cref="IDecideTypesForStrings.TypesSupported"/>.
    /// <para>There is only one instance of each <see cref="IDecideTypesForStrings"/> since they do not have any state</para>
    /// </summary>
    public class TypeDeciderFactory
    {
        /// <summary>
        /// Collection of all supported <see cref="System.Type"/> mapped to the implementation <see cref="IDecideTypesForStrings"/>.  A given 
        /// decider can support multiple raw types e.g. <see cref="IntTypeDecider"/>.
        /// </summary>
        public readonly Dictionary<Type, IDecideTypesForStrings> Dictionary = new Dictionary<Type, IDecideTypesForStrings>();

        /// <summary>
        /// Initializes a new factory and populates decider <see cref="Dictionary"/>
        /// </summary>
        public TypeDeciderFactory()
        {
            var deciders = new IDecideTypesForStrings[]
            {
                new BoolTypeDecider(),
                new IntTypeDecider(),
                new DecimalTypeDecider(),

                new NeverGuessTheseTypeDecider(),

                new TimeSpanTypeDecider(),
                new DateTimeTypeDecider(),
            };

            foreach (IDecideTypesForStrings decider in deciders)
                foreach (Type type in decider.TypesSupported)
                    Dictionary.Add(type, decider);
        }

        /// <summary>
        /// Creates a new <see cref="IDecideTypesForStrings"/> for the given <paramref name="forDataType"/>
        /// </summary>
        /// <param name="forDataType"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public IDecideTypesForStrings Create(Type forDataType)
        {
            if(!Dictionary.ContainsKey(forDataType))
                throw new NotSupportedException("DataType " + forDataType + " does not have an associated IDecideTypesForStrings");

            return Dictionary[forDataType];
        }

        /// <summary>
        /// Returns true if the <paramref name="forDataType"/> can be mapped to an implementation of <see cref="IDecideTypesForStrings"/>
        /// </summary>
        /// <param name="forDataType"></param>
        /// <returns></returns>
        public bool IsSupported(Type forDataType)
        {
            return Dictionary.ContainsKey(forDataType);
        }
    }
}
