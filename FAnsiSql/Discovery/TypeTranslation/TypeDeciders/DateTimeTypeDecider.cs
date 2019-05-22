using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace FAnsi.Discovery.TypeTranslation.TypeDeciders
{
    public class DateTimeTypeDecider : DecideTypesForStrings
    {
        private TimeSpanTypeDecider _timeSpanTypeDecider = new TimeSpanTypeDecider();
        private DecimalTypeDecider _decimalChecker = new DecimalTypeDecider();

        public static string[] DateFormatsMD;
        public static string[] DateFormatsDM;
        public static string[] TimeFormats;
        
        string[] _dateFormatToUse;

        /// <summary>
        /// Setting this to false will prevent <see cref="GuessDateFormat(IEnumerable{string})"/> changing the <see cref="Culture"/> e.g. when
        /// inserting date times
        /// </summary>
        public static bool AllowCultureGuessing = true;

        public CultureInfo Culture { get{ return culture;}
            set
                {
                    if(value.DateTimeFormat.ShortDatePattern.IndexOf('M') > value.DateTimeFormat.ShortDatePattern.IndexOf('d'))
                        _dateFormatToUse = DateFormatsDM;
                    else
                        _dateFormatToUse = DateFormatsMD;

                    culture = value; 
                } }

        static DateTimeTypeDecider()
        {
            List<string> dateFormatsMD = new List<string>();
            List<string> dateFormatsDM = new List<string>();
            List<string> timeFormats = new List<string>();

            //all dates on thier own
            foreach (string y in YearFormats)
                foreach (string M in MonthFormats)
                    foreach (string d in DayFormats)
                        foreach (string dateSeparator in DateSeparators)
                        {
                            dateFormatsMD.Add(string.Join(dateSeparator, M, d, y));
                            dateFormatsMD.Add(string.Join(dateSeparator, y, M, d));

                            dateFormatsDM.Add(string.Join(dateSeparator, d, M, y));
                            dateFormatsMD.Add(string.Join(dateSeparator, y, M, d));
                        }

            //then all the times
            foreach (string timeSeparator in TimeSeparators)
                foreach (string suffix in Suffixes)
                    foreach (string h in HourFormats)
                        foreach (string m in MinuteFormats)
                        {
                            timeFormats.Add(string.Join(timeSeparator, h, m));
                            timeFormats.Add(string.Join(timeSeparator, h, m) + " " + suffix);

                            foreach (string s in SecondFormats)
                            {
                                timeFormats.Add(string.Join(timeSeparator, h, m, s));
                                timeFormats.Add(string.Join(timeSeparator, h, m, s) + " " + suffix);
                            }
                        }
            DateFormatsDM = dateFormatsDM.ToArray();
            DateFormatsMD = dateFormatsMD.ToArray();
            TimeFormats = timeFormats.ToArray();
        }

        static string[] YearFormats = new string[]
        {
            "yy",
            "yyy",
            "yyyy",
            "yyyyy"
        };

        static string[] MonthFormats = new string[]
        {
            "M",
            "MM",
            "MMM",
            "MMMM"
        };

        static string[] DayFormats = new string[]
        {
            "dd",
            "ddd",
            "dddd"
        };

        static string[] DateSeparators = new string[]
        {
            "\\\\",
            "/",
            "-",
            "."
        };

        static string[] HourFormats = new string[]
        {
            "h",
            "hh",
            "H",
            "HH"
        };

        static string[] MinuteFormats = new string[]
        {
            "m",
            "mm"
        };

        static string[] SecondFormats = new string[]
        {
            "s",
            "ss"
        };

        static string[] Suffixes = new string[]
        {
            "tt"
        };

        static string[] TimeSeparators = new string[]
        {
            ":"
        };
        private CultureInfo culture;

        public DateTimeTypeDecider() : base(TypeCompatibilityGroup.Exclusive, typeof(DateTime))
        {
            Culture = CultureInfo.CurrentCulture;
        }

        protected override object ParseImpl(string value)
        {
            if (!TryBruteParse(value, out DateTime dt))
                throw new FormatException("Could not parse '" + value + "' to a valid DateTime");

            return dt;
        }

        /// <summary>
        /// Makes guess about whether to use MD or DM based on the <paramref name="samples"/>.
        /// Where no samples, or no matches or the same number of matches DM is used.
        /// the samples
        /// </summary>
        public void GuessDateFormat(IEnumerable<string> samples)
        {
            if(!AllowCultureGuessing)
                return;
            
            samples = samples.Where(s=>!string.IsNullOrWhiteSpace(s)).ToList();

            //if they are all valid anyway
            if(samples.All(s=>DateTime.TryParse(s,Culture,DateTimeStyles.None,out _)));
                return;
                        
            _dateFormatToUse = DateFormatsDM;
            int countDM = samples.Count(s=>TryBruteParse(s,out _));
            _dateFormatToUse = DateFormatsMD;
            int countMD = samples.Count(s=>TryBruteParse(s,out _));

            if(countDM >= countMD)
                _dateFormatToUse = DateFormatsDM;
        }

        protected override bool IsAcceptableAsTypeImpl(string candidateString, DecimalSize sizeRecord)
        {
            //if it's a float then it isn't a date is it! thanks C# for thinking 1.1 is the first of January
            if (_decimalChecker.IsAcceptableAsType(candidateString, sizeRecord))
                return false;

            //likewise if it is just the Time portion of the date then we have a column with mixed dates and times which SQL will not deal with well in the end database (e.g. it will set the
            //date portion of times to todays date which will be very confusing
            if (_timeSpanTypeDecider.IsAcceptableAsType(candidateString, sizeRecord))
                return false;

            try
            {
                return TryBruteParse(candidateString, out _);
            }
            catch (Exception)
            {
                return false;
            }
        }

        private readonly char[] space = new char[] { ' ' };

        private bool TryBruteParse(string s, out DateTime dt)
        {
            //if it's legit according to the current culture
            if(DateTime.TryParse(s,Culture,DateTimeStyles.None,out dt))
                return true;

            var split = s.Split(space, StringSplitOptions.RemoveEmptyEntries);

            //if there are no tokens
            if (split.Length == 0)
            {
                dt = DateTime.MinValue;
                return false;
            }

            //if there is one token it is assumed either to be a date or a string
            if (split.Length == 1)
                if (TryGetTime(split[0], out dt))
                {
                    return true;
                }
                else
                if (TryGetDate(split[0], out dt))
                {
                    return true;
                }
                else
                    return false;

            //if there are 2+ tokens then first token should be a date then the rest (concatenated) should be a time
            //e.g. "28/2/1993 5:36:27 AM" gets evaluated as "28/2/1993" and then "5:36:27 AM"

            if (TryGetDate(split[0], out dt) && TryGetTime(string.Join(" ", split.Skip(1)), out DateTime time))
            {
                dt = new DateTime(dt.Year, dt.Month, dt.Day, time.Hour, time.Minute, time.Second, time.Millisecond);

                return true;
            }

            dt = DateTime.MinValue;
            return false;
        }

        private bool TryGetDate(string v, out DateTime date)
        {
            return DateTime.TryParseExact(v, DateFormatsDM, Culture, DateTimeStyles.AllowInnerWhite, out date);
        }

        private bool TryGetTime(string v, out DateTime time)
        {
            return DateTime.TryParseExact(v, TimeFormats, Culture, DateTimeStyles.AllowInnerWhite, out time);
        }
    }
}