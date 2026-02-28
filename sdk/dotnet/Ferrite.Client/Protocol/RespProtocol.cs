using System.Buffers;
using System.Globalization;
using System.Net.Sockets;
using System.Text;

namespace Ferrite.Client.Protocol;

/// <summary>
/// RESP2 protocol encoder and decoder.
/// </summary>
internal static class RespProtocol
{
    private static readonly byte[] Crlf = "\r\n"u8.ToArray();

    /// <summary>
    /// Writes a RESP command as an array of bulk strings.
    /// </summary>
    public static async Task WriteCommandAsync(Stream stream, string[] args, CancellationToken ct = default)
    {
        var sb = new StringBuilder();
        sb.Append('*').Append(args.Length).Append("\r\n");
        foreach (var arg in args)
        {
            var bytes = Encoding.UTF8.GetByteCount(arg);
            sb.Append('$').Append(bytes).Append("\r\n");
            sb.Append(arg).Append("\r\n");
        }

        var data = Encoding.UTF8.GetBytes(sb.ToString());
        await stream.WriteAsync(data, ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a single RESP value. Returns string, long, object?[] or null.
    /// </summary>
    public static async Task<object?> ReadValueAsync(StreamReader reader, CancellationToken ct = default)
    {
        var line = await reader.ReadLineAsync(ct).ConfigureAwait(false)
                   ?? throw new IOException("Connection closed");

        if (line.Length == 0)
            throw new IOException("Empty RESP line");

        return line[0] switch
        {
            '+' => line[1..],
            '-' => throw new FerriteException(line[1..]),
            ':' => long.Parse(line[1..], CultureInfo.InvariantCulture),
            '$' => await ReadBulkStringAsync(reader, int.Parse(line[1..], CultureInfo.InvariantCulture), ct).ConfigureAwait(false),
            '*' => await ReadArrayAsync(reader, int.Parse(line[1..], CultureInfo.InvariantCulture), ct).ConfigureAwait(false),
            _ => throw new IOException($"Unknown RESP type: {line[0]}")
        };
    }

    private static async Task<string?> ReadBulkStringAsync(StreamReader reader, int length, CancellationToken ct)
    {
        if (length == -1) return null;

        var buf = new char[length + 2]; // +2 for \r\n
        var read = 0;
        while (read < buf.Length)
        {
            var n = await reader.ReadAsync(buf.AsMemory(read), ct).ConfigureAwait(false);
            if (n == 0) throw new IOException("Unexpected end of stream");
            read += n;
        }
        return new string(buf, 0, length);
    }

    private static async Task<object?[]?> ReadArrayAsync(StreamReader reader, int count, CancellationToken ct)
    {
        if (count == -1) return null;

        var arr = new object?[count];
        for (var i = 0; i < count; i++)
        {
            arr[i] = await ReadValueAsync(reader, ct).ConfigureAwait(false);
        }
        return arr;
    }
}

/// <summary>
/// Exception thrown when the Ferrite server returns a RESP error.
/// </summary>
public class FerriteException : Exception
{
    public FerriteException(string message) : base(message) { }
}
