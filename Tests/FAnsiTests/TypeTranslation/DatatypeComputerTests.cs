using System;
using System.Data.SqlClient;
using System.Globalization;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Implementations.MicrosoftSQL;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using Oracle.ManagedDataAccess.Client;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation
{
    /// <summary>
    /// <para>These tests cover the systems ability to compute a final <see cref="DatabaseTypeRequest"/> from a set of mixed data types.</para>
    /// 
    /// <para>Critically it covers fallback from one data type estimate to another based on new data e.g. if you see a "100" then a "1" then a "1.1"
    /// the final estimate should be decimal(4,1) to allow for both 100.0f and 1.1f.
    /// </para> 
    /// </summary>
    public class GuesserTests
    {
        readonly ITypeTranslater _translater = new MicrosoftSQLTypeTranslater();

        [Test]
        public void TestGuesser_IntToFloat()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("12");
            
            Assert.AreEqual(typeof(int),t.Guess.CSharpType);
            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(2, t.Guess.Size.NumbersBeforeDecimalPlace);

            t.AdjustToCompensateForValue("0.1");

            Assert.AreEqual(typeof(decimal), t.Guess.CSharpType);
            Assert.AreEqual(1, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(2, t.Guess.Size.NumbersBeforeDecimalPlace);
        }


        [Test]
        public void TestGuesser_IntToDate()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("12");

            Assert.AreEqual(typeof(int), t.Guess.CSharpType);
            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(2, t.Guess.Size.NumbersBeforeDecimalPlace);
            Assert.AreEqual(2, t.Guess.Width);

            t.AdjustToCompensateForValue("2001-01-01");

            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(10, t.Guess.Width);
        }

        [Test]
        public void TestGuesser_decimal()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("1.5");
            t.AdjustToCompensateForValue("299.99");
            t.AdjustToCompensateForValue(null);
            t.AdjustToCompensateForValue(DBNull.Value);

            Assert.AreEqual(typeof(decimal),t.Guess.CSharpType);
        }

        [Test]
        public void TestGuesser_Int()
        {
            Guesser t = new Guesser();
            
            t.AdjustToCompensateForValue("0");
            Assert.AreEqual(typeof(bool), t.Guess.CSharpType);
            Assert.AreEqual(1, t.Guess.Width);

            t.AdjustToCompensateForValue("-0");
            Assert.AreEqual(typeof(int), t.Guess.CSharpType);
            Assert.AreEqual(2, t.Guess.Width);
            
            
            t.AdjustToCompensateForValue("15");
            t.AdjustToCompensateForValue("299");
            t.AdjustToCompensateForValue(null);
            t.AdjustToCompensateForValue(DBNull.Value);

            Assert.AreEqual(typeof(int),t.Guess.CSharpType);
        }


        [Test]
        public void TestGuesser_IntAnddecimal_MustUsedecimal()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("15");
            t.AdjustToCompensateForValue("29.9");
            t.AdjustToCompensateForValue("200");
            t.AdjustToCompensateForValue(null);
            t.AdjustToCompensateForValue(DBNull.Value);

            Assert.AreEqual(t.Guess.CSharpType, typeof(decimal));
            var sqlType = t.GetSqlDBType(_translater);
            Assert.AreEqual("decimal(4,1)",sqlType) ;

            var orig = t.Guess;
            var reverseEngineered = _translater.GetDataTypeRequestForSQLDBType(sqlType);
            Assert.AreEqual(orig,reverseEngineered ,"The computed DataTypeRequest was not the same after going via sql datatype and reverse engineering");
        }
        [Test]
        public void TestGuesser_IntAnddecimal_MustUsedecimalThenString()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("15");
            t.AdjustToCompensateForValue("29.9");
            t.AdjustToCompensateForValue("200");
            t.AdjustToCompensateForValue(null);
            t.AdjustToCompensateForValue(DBNull.Value);
            
            Assert.AreEqual("decimal(4,1)", t.GetSqlDBType(_translater));
            t.AdjustToCompensateForValue("D");
            Assert.AreEqual("varchar(5)", t.GetSqlDBType(_translater));
        }

        public void TestGuesser_DateTimeFromInt()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("01/01/2001");
            Assert.AreEqual(typeof(DateTime), t.Guess.CSharpType);

            t.AdjustToCompensateForValue("2013");
            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
        }

        //Tests system being happy to sign off in the orders bool=>int=>decimal but nothing else
        [TestCase("true", typeof(bool), "11", typeof(int))]
        [TestCase("1", typeof(bool), "1.1",typeof(decimal))]
        [TestCase("true", typeof(bool), "1.1", typeof(decimal))]
        public void TestGuesser_FallbackCompatible(string input1, Type expectedTypeAfterFirstInput, string input2, Type expectedTypeAfterSecondInput)
        {
            var t = new Guesser();
            t.AdjustToCompensateForValue(input1);
            
            Assert.AreEqual(expectedTypeAfterFirstInput,t.Guess.CSharpType);
            
            t.AdjustToCompensateForValue(input2);
            Assert.AreEqual(expectedTypeAfterSecondInput, t.Guess.CSharpType);
        }

        //Tests system being angry at having signed off on a bool=>int=>decimal then seeing a valid non string type (e.g. DateTime)
        //under these circumstances it should go directly to System.String
        [TestCase("1",typeof(bool),"2001-01-01")]
        [TestCase("true", typeof(bool), "2001-01-01")]
        [TestCase("1.1", typeof(decimal), "2001-01-01")]
        [TestCase("1.1", typeof(decimal), "10:00am")]
        [TestCase("2001-1-1", typeof(DateTime), "10:00am")]
        public void TestGuesser_FallbackIncompatible(string input1, Type expectedTypeAfterFirstInput, string input2)
        {
            var t = new Guesser();
            t.AdjustToCompensateForValue(input1);

            Assert.AreEqual(expectedTypeAfterFirstInput, t.Guess.CSharpType);

            t.AdjustToCompensateForValue(input2);
            Assert.AreEqual(typeof(string), t.Guess.CSharpType);

            //now check it in reverse just to be sure
            t = new Guesser();
            t.AdjustToCompensateForValue(input2);
            t.AdjustToCompensateForValue(input1);
            Assert.AreEqual(typeof(string),t.Guess.CSharpType);
        }

        [Test]
        public void TestGuesser_IntToDateTime()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("2013");
            t.AdjustToCompensateForValue("01/01/2001");
            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
        }

        [TestCase("fish",32)]
        [TestCase(32, "fish")]
        [TestCase("2001-01-01",2001)]
        [TestCase(2001, "2001-01-01")]
        [TestCase("2001", 2001)]
        [TestCase(2001, "2001")]
        public void TestGuesser_MixingTypes_ThrowsException(object o1, object o2)
        {
            //if we pass an hard type...
            //...then we don't accept strings anymore

            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(o1); 

            var ex = Assert.Throws<MixedTypingException>(() => t.AdjustToCompensateForValue(o2)); 
            StringAssert.Contains("mixed with untyped objects",ex.Message);
        }

        [Test]
        public void TestGuesser_DateTime()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("01/01/2001");
            t.AdjustToCompensateForValue(null);

            Assert.AreEqual(t.Guess.CSharpType, typeof(DateTime));
            Assert.AreEqual("datetime2", t.GetSqlDBType(_translater));
        }

        [TestCase("1. 01 ", typeof(DateTime))]
        [TestCase("1. 1 ", typeof(DateTime))]
        public void TestGuesser_DateTime_DodgyFormats(string input, Type expectedOutput)
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(input);
            Assert.AreEqual(expectedOutput, t.Guess.CSharpType);
        }

        [Test]
        public void TestGuesser_DateTime_English()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(GetCultureSpecificDate());
            t.AdjustToCompensateForValue(null);

            Assert.AreEqual(t.Guess.CSharpType, typeof(DateTime));
            Assert.AreEqual("datetime2", t.GetSqlDBType(_translater));
        }

        [Test]
        public void TestGuesser_DateTime_EnglishWithTime()
        {
            Guesser t = new Guesser();
            
            Console.WriteLine(CultureInfo.CurrentCulture.EnglishName);
            Console.WriteLine(CultureInfo.CurrentCulture.DateTimeFormat.MonthDayPattern);

            t.AdjustToCompensateForValue(GetCultureSpecificDate() + " 11:10");
            t.AdjustToCompensateForValue(null);

            Assert.AreEqual(t.Guess.CSharpType, typeof(DateTime));
            Assert.AreEqual("datetime2", t.GetSqlDBType(_translater));
        }

        private string GetCultureSpecificDate()
        {
            if (CultureInfo.CurrentCulture.EnglishName.Contains("United States"))
                return "01/23/2001";

            if (CultureInfo.CurrentCulture.EnglishName.Contains("Kingdom"))
                return "23/01/2001";

            Assert.Inconclusive("Did not have a good implementation of test date for culture " +CultureInfo.CurrentCulture.EnglishName);
            return null;
        }

        [Test]
        public void TestGuesser_DateTime_EnglishWithTimeAndAM()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(GetCultureSpecificDate()+" 11:10AM");
            t.AdjustToCompensateForValue(null);

            Assert.AreEqual(t.Guess.CSharpType, typeof(DateTime));
            Assert.AreEqual("datetime2", t.GetSqlDBType(_translater));
        }

        [TestCase("01",2)]
        [TestCase("01.1", 4)]
        [TestCase("01.10", 5)]
        [TestCase("-01", 3)]
        [TestCase("-01.01", 6)]
        [TestCase(" -01.01", 7)]
        [TestCase("\t-01.01", 7)]
        [TestCase("\r\n-01.01", 8)]
        [TestCase("- 01.01", 7)]
        [TestCase(" -01.01 ", 8)]
        [TestCase("-01.01 ", 7)]
        [TestCase("--01", 4)]
        public void TestGuesser_PreeceedingZeroes(string input, int expectedLength)
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(input);
            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
            Assert.AreEqual(expectedLength, t.Guess.Width);
        }

        [Test]
        public void TestGuesser_PreeceedingZeroesAfterFloat()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("1.5");
            t.AdjustToCompensateForValue("00299.99");
            t.AdjustToCompensateForValue(null);
            t.AdjustToCompensateForValue(DBNull.Value);

            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
        }
        [Test]
        public void TestGuesser_Negatives()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("-1");
            t.AdjustToCompensateForValue("-99.99");

            Assert.AreEqual(t.Guess.CSharpType, typeof(decimal));
            Assert.AreEqual("decimal(4,2)", t.GetSqlDBType(_translater));
        }


        [Test]
        public void TestGuesser_Doubles()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(299.99);
            
            Assert.AreEqual(typeof(double), t.Guess.CSharpType);

            Assert.AreEqual(2, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(3, t.Guess.Size.NumbersBeforeDecimalPlace);
        }

        [TestCase(" 1.01", typeof(decimal))]
        [TestCase(" 1.01 ", typeof(decimal))]
        [TestCase(" 1", typeof(int))]
        [TestCase(" true ",typeof(bool))]
        public void TestGuesser_Whitespace(string input, Type expectedType)
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(input);

            Assert.AreEqual(expectedType, t.Guess.CSharpType);
            Assert.AreEqual(input.Length,t.Guess.Width);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestGuesser_Bool(bool sendStringEquiv)
        {
            Guesser t = new Guesser();

            if (sendStringEquiv)
                t.AdjustToCompensateForValue("True");
            else
                t.AdjustToCompensateForValue(true);
            
            if (sendStringEquiv)
                t.AdjustToCompensateForValue("False");
            else
                t.AdjustToCompensateForValue(false);
            
            Assert.AreEqual(typeof(bool), t.Guess.CSharpType);

            t.AdjustToCompensateForValue(null);

            Assert.AreEqual(typeof(bool), t.Guess.CSharpType);

            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(0, t.Guess.Size.NumbersBeforeDecimalPlace);
        }

        [Test]
        public void TestGuesser_MixedIntTypes()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue((Int16)5);
            var ex = Assert.Throws<MixedTypingException>(()=>t.AdjustToCompensateForValue((Int32)1000));

            StringAssert.Contains("We were adjusting to compensate for object '1000' which is of Type 'System.Int32', we were previously passed a 'System.Int16' type",ex.Message);
        }
        [Test]
        public void TestGuesser_Int16s()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue((Int16)5);
            t.AdjustToCompensateForValue((Int16)10);
            t.AdjustToCompensateForValue((Int16)15);
            t.AdjustToCompensateForValue((Int16)30);
            t.AdjustToCompensateForValue((Int16)200);

            Assert.AreEqual(typeof(Int16), t.Guess.CSharpType);

            Assert.AreEqual(3, t.Guess.Size.NumbersBeforeDecimalPlace);
            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace);
            

        }
        [Test]
        public void TestGuesser_Byte()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(new byte[5]);

            Assert.AreEqual(typeof(byte[]), t.Guess.CSharpType);

            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(0, t.Guess.Size.NumbersBeforeDecimalPlace);
            Assert.IsTrue(t.Guess.Size.IsEmpty);
        }


        [Test]
        public void TestGuesser_NumberOfDecimalPlaces()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("111111111.11111111111115");

            Assert.AreEqual(typeof(decimal), t.Guess.CSharpType);
            Assert.AreEqual(9, t.Guess.Size.NumbersBeforeDecimalPlace);
            Assert.AreEqual(14, t.Guess.Size.NumbersAfterDecimalPlace);
        }
        

        [Test]
        public void TestGuesser_TrailingZeroesFallbackToString()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("-111.000");
            
            Assert.AreEqual(typeof(int), t.Guess.CSharpType);
            Assert.AreEqual(3, t.Guess.Size.NumbersBeforeDecimalPlace);

            //even though they are trailing zeroes we still need this much space... there must be a reason why they are there right? (also makes it easier to go to string later if needed eh!)
            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace); 
            
            t.AdjustToCompensateForValue("P");

            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
            Assert.AreEqual(8, t.Guess.Width);
        }

        [Test]
        public void TestGuesser_IntFloatString()
        {
            MicrosoftSQLTypeTranslater tt = new MicrosoftSQLTypeTranslater();

            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("-1000");

            Assert.AreEqual("int",t.GetSqlDBType(tt));

            t.AdjustToCompensateForValue("1.1");
            Assert.AreEqual("decimal(5,1)", t.GetSqlDBType(tt));

            t.AdjustToCompensateForValue("A");
            Assert.AreEqual("varchar(6)", t.GetSqlDBType(tt));
        }

        [Test]
        public void TestGuesser_FallbackOntoVarcharFromFloat()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("15.5");
            t.AdjustToCompensateForValue("F");

            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
            Assert.AreEqual("varchar(4)", t.GetSqlDBType(_translater));
        }
        [Test]
        public void TestGuesser_Time()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("12:30:00");

            Assert.AreEqual(typeof(TimeSpan), t.Guess.CSharpType);
            Assert.AreEqual("time", t.GetSqlDBType(_translater));
        }

        [Test]
        public void TestGuesser_TimeNoSeconds()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("12:01");

            Assert.AreEqual(typeof(TimeSpan), t.Guess.CSharpType);
            Assert.AreEqual("time", t.GetSqlDBType(_translater));
        }

        [Test]
        public void TestGuesser_TimeWithPM()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("1:01PM");

            Assert.AreEqual(typeof(TimeSpan), t.Guess.CSharpType);
            Assert.AreEqual("time", t.GetSqlDBType(_translater));
        }
        [Test]
        public void TestGuesser_24Hour()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("23:01");

            Assert.AreEqual(typeof(TimeSpan), t.Guess.CSharpType);
            Assert.AreEqual("time", t.GetSqlDBType(_translater));
        }
        [Test]
        public void TestGuesser_Midnight()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("00:00");

            Assert.AreEqual(typeof(TimeSpan), t.Guess.CSharpType);
            Assert.AreEqual("time", t.GetSqlDBType(_translater));
        }
        [Test]
        public void TestGuesser_TimeObject()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(new TimeSpan(10,1,1));

            Assert.AreEqual(typeof(TimeSpan), t.Guess.CSharpType);
            Assert.AreEqual("time", t.GetSqlDBType(_translater));
        }
        [Test]
        public void TestGuesser_MixedDateAndTime_FallbackToString()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue("09:01");
            Assert.AreEqual(typeof(TimeSpan), t.Guess.CSharpType);

            t.AdjustToCompensateForValue("2001-12-29 23:01");
            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
            Assert.AreEqual("varchar(16)", t.GetSqlDBType(_translater));
        }

        [TestCase("1-1000")]
        public void TestGuesser_ValidDateStrings(string wierdDateString)
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(wierdDateString);
            Assert.AreEqual(typeof(DateTime), t.Guess.CSharpType);
        }

        [Test]
        public void TestGuesser_HardTypeFloats()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(1.1f);
            t.AdjustToCompensateForValue(100.01f);
            t.AdjustToCompensateForValue(10000f);

            Assert.AreEqual(typeof(float), t.Guess.CSharpType);
            Assert.AreEqual(2,t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(5, t.Guess.Size.NumbersBeforeDecimalPlace);
        }

        [Test]
        public void TestGuesser_HardTypeInts()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(1);
            t.AdjustToCompensateForValue(100);
            t.AdjustToCompensateForValue(null);
            t.AdjustToCompensateForValue(10000);
            t.AdjustToCompensateForValue(DBNull.Value);

            Assert.AreEqual(typeof(int), t.Guess.CSharpType);
            Assert.AreEqual(0, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(5, t.Guess.Size.NumbersBeforeDecimalPlace);
        }


        [Test]
        public void TestGuesser_HardTypeDoubles()
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(1.1);
            t.AdjustToCompensateForValue(100.203);
            t.AdjustToCompensateForValue(100.20000);
            t.AdjustToCompensateForValue(null);
            t.AdjustToCompensateForValue(10000d);//<- d is required because Types must be homogenous
            t.AdjustToCompensateForValue(DBNull.Value);

            Assert.AreEqual(typeof(double), t.Guess.CSharpType);
            Assert.AreEqual(3, t.Guess.Size.NumbersAfterDecimalPlace);
            Assert.AreEqual(5, t.Guess.Size.NumbersBeforeDecimalPlace);
        }


        [TestCase("0.01",typeof(decimal),"A",4)]
        [TestCase("1234",typeof(int),"FF",4)]
        [TestCase("false",typeof(bool), "FF", 5)]
        [TestCase("2001-01-01",typeof(DateTime), "FF", 27)]
        [TestCase("2001-01-01",typeof(DateTime), "FingersMcNultyFishBonesdlsiea", 29)]
        public void TestGuesser_FallbackOntoStringLength(string legitType, Type expectedLegitType, string str, int expectedLength)
        {
            Guesser t = new Guesser();
            
            //give it the legit hard typed value e.g. a date
            t.AdjustToCompensateForValue(legitType);
            Assert.AreEqual(expectedLegitType, t.Guess.CSharpType);

            //then give it a string
            t.AdjustToCompensateForValue(str);
            Assert.AreEqual(typeof(string), t.Guess.CSharpType);

            //the length should be the max of the length of the legit string and the string str
            Assert.AreEqual(expectedLength, t.Guess.Width);
            
        }

        [Test]
        [TestCase("-/-")]
        [TestCase("0/0")]
        [TestCase(".")]
        [TestCase("/")]
        [TestCase("-")]
        public void TestGuesser_RandomCrud(string randomCrud)
        {
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(randomCrud);
            Assert.AreEqual(typeof(string), t.Guess.CSharpType);
        }
        
        [Test]
        public void TestGuesser_ScientificNotation()
        {
            string val = "-4.10235746055587E-05"; //-0.0000410235746055587
            Guesser t = new Guesser();
            t.AdjustToCompensateForValue(val);
            Assert.AreEqual(typeof(decimal), t.Guess.CSharpType);
            
            Assert.AreEqual(0, t.Guess.Size.NumbersBeforeDecimalPlace);
            Assert.AreEqual(19, t.Guess.Size.NumbersAfterDecimalPlace);
        }

        [TestCase("didn’t")]
        [TestCase("Æther")]
        [TestCase("乗")]
        public void Test_NonAscii_CharacterLength(string word)
        {
            var t = new Guesser();
            t.AdjustToCompensateForValue(word);
            
            //computer should have picked up that it needs unicode
            Assert.IsTrue(t.Guess.Unicode);

            //in most DBMS
            Assert.AreEqual(t.Guess.Width,word.Length);

            //in the world of Oracle where you need varchar2(6) to store "It’s"
            t = new Guesser(){ExtraLengthPerNonAsciiCharacter = 3};
            t.AdjustToCompensateForValue(word);

            Assert.AreEqual(word.Length + 3, t.Guess.Width);
        }
    }
}
