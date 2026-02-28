using System.Globalization;

namespace Ferrite.Client;

/// <summary>
/// Client for Ferrite's FerriteQL query language.
/// </summary>
public sealed class QueryClient
{
    private readonly FerriteClient _client;

    public QueryClient(FerriteClient client)
    {
        _client = client;
    }

    /// <summary>
    /// Executes a FerriteQL query and returns the results.
    /// </summary>
    public async Task<QueryResult> ExecuteAsync(string query, CancellationToken ct = default)
    {
        var start = DateTime.UtcNow;
        var val = await _client.ExecuteAsync(ct, "QUERY", query).ConfigureAwait(false);
        var duration = DateTime.UtcNow - start;

        if (val is not object?[] arr || arr.Length == 0)
            return new QueryResult(Array.Empty<Dictionary<string, object?>>(), 0, duration);

        // First element is column headers
        var columns = new List<string>();
        if (arr[0] is object?[] headers)
        {
            columns.AddRange(headers.Select(h => h?.ToString() ?? ""));
        }

        // Remaining elements are rows
        var rows = new List<Dictionary<string, object?>>();
        for (var i = 1; i < arr.Length; i++)
        {
            if (arr[i] is not object?[] rowArr) continue;
            var row = new Dictionary<string, object?>();
            for (var j = 0; j < columns.Count && j < rowArr.Length; j++)
            {
                row[columns[j]] = ParseValue(rowArr[j]);
            }
            rows.Add(row);
        }

        return new QueryResult(rows.ToArray(), rows.Count, duration);
    }

    private static object? ParseValue(object? val)
    {
        if (val is not string s) return val;
        if (long.TryParse(s, CultureInfo.InvariantCulture, out var l)) return l;
        if (double.TryParse(s, CultureInfo.InvariantCulture, out var d)) return d;
        return s;
    }
}

/// <summary>
/// Holds the result of a FerriteQL query.
/// </summary>
public sealed record QueryResult(
    Dictionary<string, object?>[] Rows,
    long Count,
    TimeSpan Duration);
