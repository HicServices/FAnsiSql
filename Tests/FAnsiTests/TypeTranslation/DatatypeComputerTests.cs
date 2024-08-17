using System;
using System.Globalization;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Implementations.MicrosoftSQL;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation;

/// <summary>
/// <para>These tests cover the systems ability to compute a final <see cref="DatabaseTypeRequest"/> from a set of mixed data types.</para>
/// 
/// <para>Critically it covers fallback from one data type estimate to another based on new data e.g. if you see a "100" then a "1" then a "1.1"
/// the final estimate should be decimal(4,1) to allow for both 100.0f and 1.1f.
/// </para>
/// </summary>
public sealed class GuesserTests
{
    private readonly MicrosoftSQLTypeTranslater _translater = MicrosoftSQLTypeTranslater.Instance;

    [Test]
    public void TestGuesser_IntToFloat()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("12");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(int)));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(2));
        });

        t.AdjustToCompensateForValue("0.1");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(decimal)));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(1));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(2));
        });
    }


    [Test]
    public void TestGuesser_IntToDate()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("12");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(int)));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(2));
            Assert.That(t.Guess.Width, Is.EqualTo(2));
        });

        t.AdjustToCompensateForValue("2001-01-01");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Width, Is.EqualTo(10));
        });
    }

    [Test]
    public void TestGuesser_decimal()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("1.5");
        t.AdjustToCompensateForValue("299.99");
        t.AdjustToCompensateForValue(null);
        t.AdjustToCompensateForValue(DBNull.Value);

        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(decimal)));
    }

    [Test]
    public void TestGuesser_Int()
    {
        var t = new Guesser();

        t.AdjustToCompensateForValue("0");
        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(bool)));
            Assert.That(t.Guess.Width, Is.EqualTo(1));
        });

        t.AdjustToCompensateForValue("-0");
        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(int)));
            Assert.That(t.Guess.Width, Is.EqualTo(2));
        });


        t.AdjustToCompensateForValue("15");
        t.AdjustToCompensateForValue("299");
        t.AdjustToCompensateForValue(null);
        t.AdjustToCompensateForValue(DBNull.Value);

        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(int)));
    }


    [Test]
    public void TestGuesser_IntAnddecimal_MustUsedecimal()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("15");
        t.AdjustToCompensateForValue("29.9");
        t.AdjustToCompensateForValue("200");
        t.AdjustToCompensateForValue(null);
        t.AdjustToCompensateForValue(DBNull.Value);

        Assert.That(typeof(decimal), Is.EqualTo(t.Guess.CSharpType));
        var sqlType = t.GetSqlDBType(_translater);
        Assert.That(sqlType, Is.EqualTo("decimal(4,1)")) ;

        var orig = t.Guess;
        var reverseEngineered = _translater.GetDataTypeRequestForSQLDBType(sqlType);
        Assert.That(reverseEngineered, Is.EqualTo(orig), "The computed DataTypeRequest was not the same after going via sql datatype and reverse engineering");
    }
    [Test]
    public void TestGuesser_IntAndDecimal_MustUseDecimalThenString()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("15");
        t.AdjustToCompensateForValue("29.9");
        t.AdjustToCompensateForValue("200");
        t.AdjustToCompensateForValue(null);
        t.AdjustToCompensateForValue(DBNull.Value);

        Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("decimal(4,1)"));
        t.AdjustToCompensateForValue("D");
        Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("varchar(5)"));
    }

    //Tests system being happy to sign off in the orders bool=>int=>decimal but nothing else
    [TestCase("true", typeof(bool), "11", typeof(int))]
    [TestCase("1", typeof(bool), "1.1",typeof(decimal))]
    [TestCase("true", typeof(bool), "1.1", typeof(decimal))]
    public void TestGuesser_FallbackCompatible(string input1, Type expectedTypeAfterFirstInput, string input2, Type expectedTypeAfterSecondInput)
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(input1);

        Assert.That(t.Guess.CSharpType, Is.EqualTo(expectedTypeAfterFirstInput));

        t.AdjustToCompensateForValue(input2);
        Assert.That(t.Guess.CSharpType, Is.EqualTo(expectedTypeAfterSecondInput));
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

        Assert.That(t.Guess.CSharpType, Is.EqualTo(expectedTypeAfterFirstInput));

        t.AdjustToCompensateForValue(input2);
        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));

        //now check it in reverse just to be sure
        t = new Guesser();
        t.AdjustToCompensateForValue(input2);
        t.AdjustToCompensateForValue(input1);
        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
    }

    [Test]
    public void TestGuesser_IntToDateTime()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("2013");
        t.AdjustToCompensateForValue("01/01/2001");
        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
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

        var t = new Guesser();
        t.AdjustToCompensateForValue(o1);

        var ex = Assert.Throws<MixedTypingException>(() => t.AdjustToCompensateForValue(o2));
        Assert.That(ex?.Message, Does.Contain("mixed with untyped objects"));
    }

    [Test]
    public void TestGuesser_DateTime()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("01/01/2001");
        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(typeof(DateTime), Is.EqualTo(t.Guess.CSharpType));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("datetime2"));
        });
    }

    [TestCase("1. 01 ", typeof(DateTime))]
    [TestCase("1. 1 ", typeof(DateTime))]
    public void TestGuesser_DateTime_DodgyFormats(string input, Type expectedOutput)
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(input);
        Assert.That(t.Guess.CSharpType, Is.EqualTo(expectedOutput));
    }

    [Test]
    public void TestGuesser_DateTime_English()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(GetCultureSpecificDate());
        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(typeof(DateTime), Is.EqualTo(t.Guess.CSharpType));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("datetime2"));
        });
    }

    [Test]
    public void TestGuesser_DateTime_EnglishWithTime()
    {
        var t = new Guesser();

        t.AdjustToCompensateForValue($"{GetCultureSpecificDate()} 11:10");
        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(typeof(DateTime), Is.EqualTo(t.Guess.CSharpType));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("datetime2"));
        });
    }

    private static string GetCultureSpecificDate()
    {
        if (CultureInfo.CurrentCulture.EnglishName.Contains("United States"))
            return "01/23/2001";

        if (CultureInfo.CurrentCulture.EnglishName.Contains("Kingdom"))
            return "23/01/2001";

        Assert.Inconclusive(
            $"Did not have a good implementation of test date for culture {CultureInfo.CurrentCulture.EnglishName}");
        return null;
    }

    [Test]
    public void TestGuesser_DateTime_EnglishWithTimeAndAM()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue($"{GetCultureSpecificDate()} 11:10AM");
        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(typeof(DateTime), Is.EqualTo(t.Guess.CSharpType));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("datetime2"));
        });
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
        var t = new Guesser();
        t.AdjustToCompensateForValue(input);
        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(t.Guess.Width, Is.EqualTo(expectedLength));
        });
    }

    [Test]
    public void TestGuesser_PreeceedingZeroesAfterFloat()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("1.5");
        t.AdjustToCompensateForValue("00299.99");
        t.AdjustToCompensateForValue(null);
        t.AdjustToCompensateForValue(DBNull.Value);

        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
    }
    [Test]
    public void TestGuesser_Negatives()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("-1");
        t.AdjustToCompensateForValue("-99.99");

        Assert.Multiple(() =>
        {
            Assert.That(typeof(decimal), Is.EqualTo(t.Guess.CSharpType));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("decimal(4,2)"));
        });
    }


    [Test]
    public void TestGuesser_Doubles()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(299.99);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(double)));

            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(2));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(3));
        });
    }

    [TestCase(" 1.01", typeof(decimal))]
    [TestCase(" 1.01 ", typeof(decimal))]
    [TestCase(" 1", typeof(int))]
    [TestCase(" true ",typeof(bool))]
    public void TestGuesser_Whitespace(string input, Type expectedType)
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(input);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(expectedType));
            Assert.That(t.Guess.Width, Is.EqualTo(input.Length));
        });
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void TestGuesser_Bool(bool sendStringEquiv)
    {
        var t = new Guesser();

        if (sendStringEquiv)
            t.AdjustToCompensateForValue("True");
        else
            t.AdjustToCompensateForValue(true);

        if (sendStringEquiv)
            t.AdjustToCompensateForValue("False");
        else
            t.AdjustToCompensateForValue(false);

        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(bool)));

        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(bool)));

            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(0));
        });
    }

    [Test]
    public void TestGuesser_MixedIntTypes()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue((short)5);
        var ex = Assert.Throws<MixedTypingException>(()=>t.AdjustToCompensateForValue(1000));

        Assert.That(ex?.Message, Does.Contain("We were adjusting to compensate for object '1000' which is of Type 'System.Int32', we were previously passed a 'System.Int16' type"));
    }
    [Test]
    public void TestGuesser_Int16s()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue((short)5);
        t.AdjustToCompensateForValue((short)10);
        t.AdjustToCompensateForValue((short)15);
        t.AdjustToCompensateForValue((short)30);
        t.AdjustToCompensateForValue((short)200);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(short)));

            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(3));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
        });


    }
    [Test]
    public void TestGuesser_Byte()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(new byte[5]);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(byte[])));

            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Size.IsEmpty);
        });
    }


    [Test]
    public void TestGuesser_NumberOfDecimalPlaces()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("111111111.11111111111115");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(decimal)));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(9));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(14));
        });
    }


    [Test]
    public void TestGuesser_TrailingZeroesFallbackToString()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("-111.000");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(int)));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(3));

            //even though they are trailing zeroes we still need this much space... there must be a reason why they are there right? (also makes it easier to go to string later if needed eh!)
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
        });

        t.AdjustToCompensateForValue("P");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(t.Guess.Width, Is.EqualTo(8));
        });
    }

    [Test]
    public void TestGuesser_IntFloatString()
    {
        var tt = MicrosoftSQLTypeTranslater.Instance;

        var t = new Guesser();
        t.AdjustToCompensateForValue("-1000");

        Assert.That(t.GetSqlDBType(tt), Is.EqualTo("int"));

        t.AdjustToCompensateForValue("1.1");
        Assert.That(t.GetSqlDBType(tt), Is.EqualTo("decimal(5,1)"));

        t.AdjustToCompensateForValue("A");
        Assert.That(t.GetSqlDBType(tt), Is.EqualTo("varchar(6)"));
    }

    [Test]
    public void TestGuesser_FallbackOntoVarcharFromFloat()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("15.5");
        t.AdjustToCompensateForValue("F");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("varchar(4)"));
        });
    }
    [Test]
    public void TestGuesser_Time()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("12:30:00");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("time"));
        });
    }

    [Test]
    public void TestGuesser_TimeNoSeconds()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("12:01");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("time"));
        });
    }

    [Test]
    public void TestGuesser_TimeWithPM()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("1:01PM");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("time"));
        });
    }
    [Test]
    public void TestGuesser_24Hour()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("23:01");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("time"));
        });
    }
    [Test]
    public void TestGuesser_Midnight()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("00:00");

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("time"));
        });
    }
    [Test]
    public void TestGuesser_TimeObject()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(new TimeSpan(10,1,1));

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("time"));
        });
    }
    [Test]
    public void TestGuesser_MixedDateAndTime_FallbackToString()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("09:01");
        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));

        t.AdjustToCompensateForValue("2001-12-29 23:01");
        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("varchar(16)"));
        });
    }

    [TestCase("1-1000")]
    public void TestGuesser_ValidDateStrings(string wierdDateString)
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(wierdDateString);
        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(DateTime)));
    }

    [Test]
    public void TestGuesser_HardTypeFloats()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(1.1f);
        t.AdjustToCompensateForValue(100.01f);
        t.AdjustToCompensateForValue(10000f);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(float)));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(2));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(5));
        });
    }

    [Test]
    public void TestGuesser_HardTypeInts()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(1);
        t.AdjustToCompensateForValue(100);
        t.AdjustToCompensateForValue(null);
        t.AdjustToCompensateForValue(10000);
        t.AdjustToCompensateForValue(DBNull.Value);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(int)));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(5));
        });
    }


    [Test]
    public void TestGuesser_HardTypeDoubles()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(1.1);
        t.AdjustToCompensateForValue(100.203);
        t.AdjustToCompensateForValue(100.20000);
        t.AdjustToCompensateForValue(null);
        t.AdjustToCompensateForValue(10000d);//<- d is required because Types must be homogenous
        t.AdjustToCompensateForValue(DBNull.Value);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(double)));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(3));
            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(5));
        });
    }


    [TestCase("0.01",typeof(decimal),"A",4)]
    [TestCase("1234",typeof(int),"FF",4)]
    [TestCase("false",typeof(bool), "FF", 5)]
    [TestCase("2001-01-01",typeof(DateTime), "FF", 27)]
    [TestCase("2001-01-01",typeof(DateTime), "FingersMcNultyFishBonesdlsiea", 29)]
    public void TestGuesser_FallbackOntoStringLength(string legitType, Type expectedLegitType, string str, int expectedLength)
    {
        var t = new Guesser();

        //give it the legit hard typed value e.g. a date
        t.AdjustToCompensateForValue(legitType);
        Assert.That(t.Guess.CSharpType, Is.EqualTo(expectedLegitType));

        //then give it a string
        t.AdjustToCompensateForValue(str);
        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));

            //the length should be the max of the length of the legit string and the string str
            Assert.That(t.Guess.Width, Is.EqualTo(expectedLength));
        });

    }

    [Test]
    [TestCase("-/-")]
    [TestCase("0/0")]
    [TestCase(".")]
    [TestCase("/")]
    [TestCase("-")]
    public void TestGuesser_RandomCrud(string randomCrud)
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(randomCrud);
        Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(string)));
    }

    [Test]
    public void TestGuesser_ScientificNotation()
    {
        const string val = "-4.10235746055587E-05"; //-0.0000410235746055587
        var t = new Guesser();
        t.AdjustToCompensateForValue(val);
        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(decimal)));

            Assert.That(t.Guess.Size.NumbersBeforeDecimalPlace, Is.EqualTo(0));
            Assert.That(t.Guess.Size.NumbersAfterDecimalPlace, Is.EqualTo(19));
        });
    }

    [TestCase("didn’t")]
    [TestCase("Æther")]
    [TestCase("乗")]
    public void Test_NonAscii_CharacterLength(string word)
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue(word);

        Assert.Multiple(() =>
        {
            //computer should have picked up that it needs unicode
            Assert.That(t.Guess.Unicode);

            //in most DBMS
            Assert.That(word, Has.Length.EqualTo(t.Guess.Width));
        });

        //in the world of Oracle where you need varchar2(6) to store "It’s"
        t = new Guesser {ExtraLengthPerNonAsciiCharacter = 3};
        t.AdjustToCompensateForValue(word);

        Assert.That(t.Guess.Width, Is.EqualTo(word.Length + 3));
    }
}