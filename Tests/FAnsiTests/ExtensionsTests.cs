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

        var dc = dt.Columns.Add("FFF");

        Assert.That(dc.GetDoNotReType(), Is.False);

        Assert.DoesNotThrow(() => dt.SetDoNotReType(true));

        Assert.That(dc.GetDoNotReType(), Is.True);

        Assert.DoesNotThrow(() => dt.SetDoNotReType(false));

        Assert.That(dc.GetDoNotReType(), Is.False);
    }
}