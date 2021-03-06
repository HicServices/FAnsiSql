﻿using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation
{
    class DatabaseTypeRequestTests
    {
        [Test]
        public void Test_Max_WithUnicode()
        {
            
            var max = DatabaseTypeRequest.Max(
                new DatabaseTypeRequest(typeof(string), 1, null){Unicode = true},
                new DatabaseTypeRequest(typeof(string), 2, null)
                );

            Assert.AreEqual(2,max.Width);
            Assert.IsTrue(max.Unicode,"If either arg in a Max call is Unicode then the resulting maximum should be Unicode=true");
        }
    }
}
