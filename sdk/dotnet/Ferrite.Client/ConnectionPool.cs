using System.Net.Security;
using System.Net.Sockets;
using Ferrite.Client.Protocol;

namespace Ferrite.Client;

/// <summary>
/// Manages a pool of connections to a Ferrite server.
/// </summary>
internal sealed class ConnectionPool : IAsyncDisposable
{
    private readonly FerriteOptions _options;
    private readonly SemaphoreSlim _semaphore;
    private readonly System.Collections.Concurrent.ConcurrentBag<PooledConnection> _idle = new();
    private volatile bool _disposed;

    public ConnectionPool(FerriteOptions options)
    {
        _options = options;
        _semaphore = new SemaphoreSlim(options.PoolSize, options.PoolSize);
    }

    public async Task<PooledConnection> GetAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _semaphore.WaitAsync(ct).ConfigureAwait(false);

        if (_idle.TryTake(out var conn))
            return conn;

        try
        {
            return await CreateConnectionAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            _semaphore.Release();
            throw;
        }
    }

    public void Return(PooledConnection conn)
    {
        if (_disposed)
        {
            conn.Dispose();
        }
        else
        {
            _idle.Add(conn);
        }
        _semaphore.Release();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        while (_idle.TryTake(out var conn))
        {
            conn.Dispose();
        }

        _semaphore.Dispose();
    }

    private async Task<PooledConnection> CreateConnectionAsync(CancellationToken ct)
    {
        var socket = new TcpClient();
        await socket.ConnectAsync(_options.Host, _options.Port, ct).ConfigureAwait(false);

        Stream stream = socket.GetStream();
        if (_options.Ssl)
        {
            var sslStream = new SslStream(stream);
            await sslStream.AuthenticateAsClientAsync(_options.Host).ConfigureAwait(false);
            stream = sslStream;
        }

        return new PooledConnection(socket, stream);
    }
}

/// <summary>
/// A single pooled connection to a Ferrite server.
/// </summary>
internal sealed class PooledConnection : IDisposable
{
    private readonly TcpClient _client;

    public Stream Stream { get; }
    public StreamReader Reader { get; }

    public PooledConnection(TcpClient client, Stream stream)
    {
        _client = client;
        Stream = stream;
        Reader = new StreamReader(stream);
    }

    public void Dispose()
    {
        Reader.Dispose();
        Stream.Dispose();
        _client.Dispose();
    }
}
