using System.Data;
using FAnsi.Extensions;
using NUnit.Framework;

namespace FAnsiTests;

internal class ExtensionsTests
{
    [Test]
    public void Test_SetDoNotReType_RepeatCalls()
    {
        using var dt = new DataTable();

        Assert.DoesNotThrow(()=>dt.SetDoNotReType(true));

        dt.Columns.Add("FFF");

        Assert.IsFalse(dt.Columns["FFF"].GetDoNotReType());

        Assert.DoesNotThrow(()=>dt.SetDoNotReType(true));

        Assert.IsTrue(dt.Columns["FFF"].GetDoNotReType());

        Assert.DoesNotThrow(()=>dt.SetDoNotReType(false));

        Assert.IsFalse(dt.Columns["FFF"].GetDoNotReType());
    }
}