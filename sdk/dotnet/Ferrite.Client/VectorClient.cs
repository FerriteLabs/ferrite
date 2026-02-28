using System.Globalization;

namespace Ferrite.Client;

/// <summary>
/// Client for Ferrite vector search operations.
/// </summary>
public sealed class VectorClient
{
    private readonly FerriteClient _client;

    public VectorClient(FerriteClient client)
    {
        _client = client;
    }

    /// <summary>Creates a new vector index.</summary>
    public async Task CreateIndexAsync(string name, int dimension, string metric, CancellationToken ct = default)
    {
        await _client.ExecuteAsync(ct, "VECTOR.INDEX.CREATE", name,
            dimension.ToString(), metric).ConfigureAwait(false);
    }

    /// <summary>Adds a vector with optional metadata to an index.</summary>
    public async Task AddVectorAsync(string index, string id, float[] vector,
        Dictionary<string, object>? metadata = null, CancellationToken ct = default)
    {
        var args = new List<string> { "VECTOR.ADD", index, id, EncodeVector(vector) };
        if (metadata is { Count: > 0 })
        {
            args.Add(EncodeMetadata(metadata));
        }
        await _client.ExecuteAsync(ct, args.ToArray()).ConfigureAwait(false);
    }

    /// <summary>Searches for the nearest vectors.</summary>
    public async Task<VectorResult[]> SearchAsync(string index, float[] query, int topK,
        CancellationToken ct = default)
    {
        var val = await _client.ExecuteAsync(ct, "VECTOR.SEARCH", index,
            EncodeVector(query), topK.ToString()).ConfigureAwait(false);

        if (val is not object?[] arr)
            return Array.Empty<VectorResult>();

        var results = new List<VectorResult>();
        for (var i = 0; i + 2 < arr.Length; i += 3)
        {
            var rid = arr[i]?.ToString() ?? "";
            var distance = arr[i + 1] != null
                ? double.Parse(arr[i + 1]!.ToString()!, CultureInfo.InvariantCulture) : 0;
            Dictionary<string, object>? meta = null;
            if (arr[i + 2] is string metaStr && !string.IsNullOrEmpty(metaStr))
            {
                meta = ParseMetadata(metaStr);
            }
            results.Add(new VectorResult(rid, distance, meta));
        }
        return results.ToArray();
    }

    /// <summary>Deletes a vector from an index.</summary>
    public async Task DeleteAsync(string index, string id, CancellationToken ct = default)
    {
        await _client.ExecuteAsync(ct, "VECTOR.DELETE", index, id).ConfigureAwait(false);
    }

    private static string EncodeVector(float[] vector)
    {
        return string.Join(",", vector.Select(v => v.ToString(CultureInfo.InvariantCulture)));
    }

    private static string EncodeMetadata(Dictionary<string, object> metadata)
    {
        var pairs = metadata.Select(kv =>
        {
            var val = kv.Value is string s ? $"\"{Escape(s)}\"" : kv.Value.ToString();
            return $"\"{Escape(kv.Key)}\":{val}";
        });
        return "{" + string.Join(",", pairs) + "}";
    }

    private static Dictionary<string, object> ParseMetadata(string json)
    {
        var dict = new Dictionary<string, object>();
        var inner = json.Trim('{', '}', ' ');
        if (string.IsNullOrEmpty(inner)) return dict;

        foreach (var pair in inner.Split(','))
        {
            var parts = pair.Split(':', 2);
            if (parts.Length != 2) continue;
            var key = parts[0].Trim().Trim('"');
            var val = parts[1].Trim();
            if (val.StartsWith('"') && val.EndsWith('"'))
                dict[key] = val[1..^1];
            else if (double.TryParse(val, CultureInfo.InvariantCulture, out var num))
                dict[key] = num;
            else
                dict[key] = val;
        }
        return dict;
    }

    private static string Escape(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");
}

/// <summary>
/// Represents a single vector search result.
/// </summary>
public sealed record VectorResult(string Id, double Distance, Dictionary<string, object>? Metadata);
