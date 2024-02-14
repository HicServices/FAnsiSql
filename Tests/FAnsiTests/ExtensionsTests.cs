using System.Data;
using FAnsi.Extensions;
using NUnit.Framework;

namespace FAnsiTests;

internal sealed class ExtensionsTests
{
    [Test]
    public void Test_SetDoNotReType_RepeatCalls()
    {
        using var dt = new DataTable();

        Assert.DoesNotThrow(() => dt.SetDoNotReType(true));

        dt.Columns.Add("FFF");

        Assert.That(dt.Columns["FFF"]?.GetDoNotReType(), Is.False);

        Assert.DoesNotThrow(() => dt.SetDoNotReType(true));

        Assert.That(dt.Columns["FFF"]?.GetDoNotReType(), Is.True);

        Assert.DoesNotThrow(() => dt.SetDoNotReType(false));

        Assert.That(dt.Columns["FFF"]?.GetDoNotReType(), Is.False);
    }
}