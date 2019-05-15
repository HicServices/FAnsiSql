using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation
{
    class DecimalSizeTests
    {
        [Test]
        public void Test_DecimalSize_Empty()
        {
            var empty = new DecimalSize();

            Assert.IsNull(empty.NumbersAfterDecimalPlace);
            Assert.IsNull(empty.NumbersBeforeDecimalPlace);

            Assert.AreEqual(0,empty.Precision);
            Assert.AreEqual(0,empty.Scale);
            
            Assert.IsTrue(empty.IsEmpty);
        }

        
        [Test]
        public void Test_DecimalSize_Equality()
        {
            Assert.AreEqual(new DecimalSize(),new DecimalSize());
            Assert.AreEqual(new DecimalSize(),new DecimalSize(){NumbersAfterDecimalPlace = 0 });
            Assert.AreEqual(new DecimalSize(),new DecimalSize(){NumbersAfterDecimalPlace = 0 ,NumbersBeforeDecimalPlace = 0});
            Assert.AreEqual(new DecimalSize(3,4),new DecimalSize(3,4));

            Assert.AreEqual(new DecimalSize().GetHashCode(),new DecimalSize().GetHashCode());
            Assert.AreEqual(new DecimalSize().GetHashCode(),new DecimalSize(){NumbersAfterDecimalPlace = 0 }.GetHashCode());
            Assert.AreEqual(new DecimalSize().GetHashCode(),new DecimalSize(){NumbersAfterDecimalPlace = 0 ,NumbersBeforeDecimalPlace = 0}.GetHashCode());
            Assert.AreEqual(new DecimalSize(3,4).GetHashCode(),new DecimalSize(){NumbersAfterDecimalPlace = 4 ,NumbersBeforeDecimalPlace = 3}.GetHashCode());            
        }

        [Test]
        public void Test_DecimalSize_NoFraction()
        {
            //decimal(5,0)
            var size = new DecimalSize(5,0);

            Assert.AreEqual(5,size.Precision);
            Assert.AreEqual(0,size.Scale);
            
            Assert.IsFalse(size.IsEmpty);
        }
        [Test]
        public void Test_DecimalSize_SomeFraction()
        {
            //decimal(7,2)
            var size = new DecimalSize(5,2);

            Assert.AreEqual(7,size.Precision);
            Assert.AreEqual(2,size.Scale);
            
            Assert.IsFalse(size.IsEmpty);
        }

        
        [Test]
        public void Test_DecimalSize_Combine()
        {
            //decimal(3,0)
            var size1 = new DecimalSize(3,0);
            Assert.AreEqual(3,size1.Precision);
            Assert.AreEqual(0,size1.Scale);

            //decimal(5,4)
            var size2 = new DecimalSize(1,4);
            Assert.AreEqual(5,size2.Precision);
            Assert.AreEqual(4,size2.Scale);


            var combined = DecimalSize.Combine(size1,size2);
            
            Assert.AreEqual(3,combined.NumbersBeforeDecimalPlace);
            Assert.AreEqual(4,combined.NumbersAfterDecimalPlace);

            //decimal(7,4)
            Assert.AreEqual(7,combined.Precision);
            Assert.AreEqual(4,combined.Scale);
        }
    }
}
