using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation;

internal class DatabaseTypeRequestTests
{
    [Test]
    public void Test_Max_WithUnicode()
    {

        var max = DatabaseTypeRequest.Max(
            new DatabaseTypeRequest(typeof(string), 1){Unicode = true},
            new DatabaseTypeRequest(typeof(string), 2)
        );

        Assert.Multiple(() =>
        {
            Assert.That(max.Width, Is.EqualTo(2));
            Assert.That(max.Unicode, "If either arg in a Max call is Unicode then the resulting maximum should be Unicode=true");
        });
    }
}