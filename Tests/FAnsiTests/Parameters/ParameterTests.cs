﻿using System;
using FAnsi;
using FAnsi.Implementation;
using NUnit.Framework;
using System.Data;
using System.Text;
using TypeGuesser;

namespace FAnsiTests.Parameters;

internal sealed class ParameterTests:DatabaseTests
{
    [TestCaseSource(typeof(All),nameof(All.DatabaseTypes))]
    public void Test_SupportsEmbeddedParameters_DeclarationOrThrow(DatabaseType type)
    {
        var syntax = ImplementationManager.GetImplementation(type).GetQuerySyntaxHelper();

        if(syntax.SupportsEmbeddedParameters())
            Assert.That(syntax.GetParameterDeclaration("@bob",new DatabaseTypeRequest(typeof(string),10)), Is.Not.Empty);
        else
            Assert.Throws<NotSupportedException>(() =>syntax.GetParameterDeclaration("@bob", new DatabaseTypeRequest(typeof(string), 10)));
    }

    [TestCase(DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MySql)]
    //[TestCase(DatabaseType.Oracle)]
    public void CreateParameter(DatabaseType type)
    {
        var syntax = ImplementationManager.GetImplementation(type).GetQuerySyntaxHelper();

        var declaration = syntax.GetParameterDeclaration("@bob",new DatabaseTypeRequest(typeof(string),10));

        Assert.That(declaration, Does.Contain("@bob"));
    }

    [TestCase(DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MySql)]
    //[TestCase(DatabaseType.Oracle)]
    public void CreateParameter_AndUse(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var dt = new DataTable();

        dt.Columns.Add("FF");
        dt.Rows.Add("armag");
        dt.Rows.Add("geddon");

        var tbl = db.CreateTable("ParameterUseTest",dt);

        var sb = new StringBuilder();

        //declare the variable
        sb.AppendLine(tbl.GetQuerySyntaxHelper().GetParameterDeclaration("@bob",new DatabaseTypeRequest(typeof(string),10)));

        sb.AppendLine("SET @bob='armag';");
        //set the variable

        sb.AppendLine($"SELECT FF from {tbl.GetFullyQualifiedName()} WHERE FF = @bob;");

        using var con = db.Server.GetConnection();
        con.Open();
        var r = db.Server.GetCommand(sb.ToString(),con).ExecuteReader();

        Assert.That(r.Read());
        Assert.That(r.Read(), Is.False);
    }
}