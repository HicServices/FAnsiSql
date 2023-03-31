namespace FAnsi.Discovery.QuerySyntax;

/// <summary>
/// Describes how to achieve a 'Select Top X from Table' query (return only the first X matching records for the query).  It includes the SQL text required to
/// achieve it (e.g. 'Top X' in Sql Server vs 'LIMIT 10' in MySql) along with where it has to appear in the query (See QueryComponent).
/// </summary>
public class TopXResponse
{
    public string SQL { get; set; }
    public QueryComponent Location { get; set; }

    public TopXResponse(string sql, QueryComponent location)
    {
        SQL = sql;
        Location = location;
    }
}