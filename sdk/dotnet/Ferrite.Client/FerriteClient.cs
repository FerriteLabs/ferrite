using System.Globalization;
using Ferrite.Client.Protocol;

namespace Ferrite.Client;

/// <summary>
/// Main Ferrite client with async operations and connection pooling.
/// Implements <see cref="IAsyncDisposable"/> for proper resource cleanup.
/// </summary>
/// <example>
/// <code>
/// await using var client = await FerriteClient.ConnectAsync("localhost", 6379);
/// await client.SetAsync("key", "value");
/// var val = await client.GetAsync("key");
/// </code>
/// </example>
public sealed class FerriteClient : IAsyncDisposable
{
    private readonly ConnectionPool _pool;
    private readonly FerriteOptions _options;

    private FerriteClient(FerriteOptions options)
    {
        _options = options;
        _pool = new ConnectionPool(options);
    }

    /// <summary>
    /// Connects to a Ferrite server with the specified options.
    /// </summary>
    public static async Task<FerriteClient> ConnectAsync(
        string host, int port, FerriteOptions? options = null, CancellationToken ct = default)
    {
        options ??= new FerriteOptions();
        options.Host = host;
        options.Port = port;

        var client = new FerriteClient(options);

        // Validate connectivity
        await client.PingAsync(ct).ConfigureAwait(false);

        // Authenticate
        if (!string.IsNullOrEmpty(options.Password))
        {
            await client.ExecuteAsync(ct, "AUTH", options.Password).ConfigureAwait(false);
        }

        // Select database
        if (options.Database != 0)
        {
            await client.ExecuteAsync(ct, "SELECT", options.Database.ToString()).ConfigureAwait(false);
        }

        return client;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _pool.DisposeAsync().ConfigureAwait(false);
    }

    // --- Internal command execution ---

    internal async Task<object?> ExecuteAsync(CancellationToken ct, params string[] args)
    {
        Exception? lastErr = null;
        for (var attempt = 0; attempt <= _options.MaxRetries; attempt++)
        {
            if (attempt > 0)
            {
                await Task.Delay(_options.RetryBackoff * attempt, ct).ConfigureAwait(false);
            }

            var conn = await _pool.GetAsync(ct).ConfigureAwait(false);
            try
            {
                await RespProtocol.WriteCommandAsync(conn.Stream, args, ct).ConfigureAwait(false);
                var result = await RespProtocol.ReadValueAsync(conn.Reader, ct).ConfigureAwait(false);
                _pool.Return(conn);
                return result;
            }
            catch (Exception ex)
            {
                lastErr = ex;
                conn.Dispose();
            }
        }
        throw lastErr!;
    }

    // --- String operations ---

    /// <summary>Gets the value of a key. Returns null if the key does not exist.</summary>
    public async Task<string?> GetAsync(string key, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "GET", key).ConfigureAwait(false);
        return val?.ToString();
    }

    /// <summary>Sets a key to a string value.</summary>
    public async Task SetAsync(string key, string value, TimeSpan? expiry = null, CancellationToken ct = default)
    {
        if (expiry.HasValue)
        {
            await ExecuteAsync(ct, "SET", key, value, "PX",
                ((long)expiry.Value.TotalMilliseconds).ToString()).ConfigureAwait(false);
        }
        else
        {
            await ExecuteAsync(ct, "SET", key, value).ConfigureAwait(false);
        }
    }

    /// <summary>Deletes one or more keys. Returns the number removed.</summary>
    public async Task<long> DeleteAsync(CancellationToken ct = default, params string[] keys)
    {
        var args = new string[keys.Length + 1];
        args[0] = "DEL";
        Array.Copy(keys, 0, args, 1, keys.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    /// <summary>Returns the number of specified keys that exist.</summary>
    public async Task<long> ExistsAsync(CancellationToken ct = default, params string[] keys)
    {
        var args = new string[keys.Length + 1];
        args[0] = "EXISTS";
        Array.Copy(keys, 0, args, 1, keys.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    /// <summary>Sets a timeout on a key.</summary>
    public async Task<bool> ExpireAsync(string key, TimeSpan ttl, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "EXPIRE", key,
            ((long)ttl.TotalSeconds).ToString()).ConfigureAwait(false);
        return (long)val! == 1;
    }

    /// <summary>Returns the remaining TTL of a key.</summary>
    public async Task<TimeSpan> TtlAsync(string key, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "TTL", key).ConfigureAwait(false);
        return TimeSpan.FromSeconds((long)val!);
    }

    // --- List operations ---

    /// <summary>Prepends values to a list.</summary>
    public async Task<long> LPushAsync(string key, CancellationToken ct = default, params string[] values)
    {
        var args = new string[values.Length + 2];
        args[0] = "LPUSH"; args[1] = key;
        Array.Copy(values, 0, args, 2, values.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    /// <summary>Appends values to a list.</summary>
    public async Task<long> RPushAsync(string key, CancellationToken ct = default, params string[] values)
    {
        var args = new string[values.Length + 2];
        args[0] = "RPUSH"; args[1] = key;
        Array.Copy(values, 0, args, 2, values.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    /// <summary>Removes and returns the first element.</summary>
    public async Task<string?> LPopAsync(string key, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "LPOP", key).ConfigureAwait(false);
        return val?.ToString();
    }

    /// <summary>Removes and returns the last element.</summary>
    public async Task<string?> RPopAsync(string key, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "RPOP", key).ConfigureAwait(false);
        return val?.ToString();
    }

    /// <summary>Returns elements in the given range.</summary>
    public async Task<string[]> LRangeAsync(string key, long start, long stop, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "LRANGE", key, start.ToString(), stop.ToString()).ConfigureAwait(false);
        return ToStringArray(val);
    }

    /// <summary>Returns the length of a list.</summary>
    public async Task<long> LLenAsync(string key, CancellationToken ct = default)
    {
        return (long)(await ExecuteAsync(ct, "LLEN", key).ConfigureAwait(false))!;
    }

    // --- Hash operations ---

    /// <summary>Sets field-value pairs in a hash.</summary>
    public async Task<long> HSetAsync(string key, CancellationToken ct = default, params string[] fieldValues)
    {
        var args = new string[fieldValues.Length + 2];
        args[0] = "HSET"; args[1] = key;
        Array.Copy(fieldValues, 0, args, 2, fieldValues.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    /// <summary>Gets the value of a hash field.</summary>
    public async Task<string?> HGetAsync(string key, string field, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "HGET", key, field).ConfigureAwait(false);
        return val?.ToString();
    }

    /// <summary>Returns all fields and values of a hash.</summary>
    public async Task<Dictionary<string, string>> HGetAllAsync(string key, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "HGETALL", key).ConfigureAwait(false);
        return ToStringDictionary(val);
    }

    /// <summary>Deletes fields from a hash.</summary>
    public async Task<long> HDelAsync(string key, CancellationToken ct = default, params string[] fields)
    {
        var args = new string[fields.Length + 2];
        args[0] = "HDEL"; args[1] = key;
        Array.Copy(fields, 0, args, 2, fields.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    // --- Set operations ---

    /// <summary>Adds members to a set.</summary>
    public async Task<long> SAddAsync(string key, CancellationToken ct = default, params string[] members)
    {
        var args = new string[members.Length + 2];
        args[0] = "SADD"; args[1] = key;
        Array.Copy(members, 0, args, 2, members.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    /// <summary>Returns all members of a set.</summary>
    public async Task<string[]> SMembersAsync(string key, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "SMEMBERS", key).ConfigureAwait(false);
        return ToStringArray(val);
    }

    /// <summary>Removes members from a set.</summary>
    public async Task<long> SRemAsync(string key, CancellationToken ct = default, params string[] members)
    {
        var args = new string[members.Length + 2];
        args[0] = "SREM"; args[1] = key;
        Array.Copy(members, 0, args, 2, members.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    /// <summary>Returns the number of members in a set.</summary>
    public async Task<long> SCardAsync(string key, CancellationToken ct = default)
    {
        return (long)(await ExecuteAsync(ct, "SCARD", key).ConfigureAwait(false))!;
    }

    // --- Sorted Set operations ---

    /// <summary>Adds a member with a score.</summary>
    public async Task<long> ZAddAsync(string key, double score, string member, CancellationToken ct = default)
    {
        return (long)(await ExecuteAsync(ct, "ZADD", key,
            score.ToString(CultureInfo.InvariantCulture), member).ConfigureAwait(false))!;
    }

    /// <summary>Returns members in the given rank range.</summary>
    public async Task<string[]> ZRangeAsync(string key, long start, long stop, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "ZRANGE", key, start.ToString(), stop.ToString()).ConfigureAwait(false);
        return ToStringArray(val);
    }

    /// <summary>Returns the score of a member.</summary>
    public async Task<double?> ZScoreAsync(string key, string member, CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "ZSCORE", key, member).ConfigureAwait(false);
        if (val == null) return null;
        return double.Parse(val.ToString()!, CultureInfo.InvariantCulture);
    }

    /// <summary>Removes members from a sorted set.</summary>
    public async Task<long> ZRemAsync(string key, CancellationToken ct = default, params string[] members)
    {
        var args = new string[members.Length + 2];
        args[0] = "ZREM"; args[1] = key;
        Array.Copy(members, 0, args, 2, members.Length);
        return (long)(await ExecuteAsync(ct, args).ConfigureAwait(false))!;
    }

    // --- Server operations ---

    /// <summary>Sends PING to verify connectivity.</summary>
    public async Task PingAsync(CancellationToken ct = default)
    {
        await ExecuteAsync(ct, "PING").ConfigureAwait(false);
    }

    /// <summary>Returns server information.</summary>
    public async Task<string> InfoAsync(CancellationToken ct = default)
    {
        var val = await ExecuteAsync(ct, "INFO").ConfigureAwait(false);
        return val?.ToString() ?? "";
    }

    // --- Helpers ---

    private static string[] ToStringArray(object? val)
    {
        if (val is not object?[] arr) return Array.Empty<string>();
        return arr.Select(o => o?.ToString() ?? "").ToArray();
    }

    private static Dictionary<string, string> ToStringDictionary(object? val)
    {
        if (val is not object?[] arr) return new Dictionary<string, string>();
        var dict = new Dictionary<string, string>();
        for (var i = 0; i < arr.Length - 1; i += 2)
        {
            var k = arr[i]?.ToString() ?? "";
            var v = arr[i + 1]?.ToString() ?? "";
            dict[k] = v;
        }
        return dict;
    }
}
