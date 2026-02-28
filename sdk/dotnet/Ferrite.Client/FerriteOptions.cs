namespace Ferrite.Client;

/// <summary>
/// Connection options for a Ferrite client.
/// </summary>
public sealed class FerriteOptions
{
    /// <summary>Server hostname. Default: "localhost".</summary>
    public string Host { get; set; } = "localhost";

    /// <summary>Server port. Default: 6379.</summary>
    public int Port { get; set; } = 6379;

    /// <summary>Authentication password.</summary>
    public string? Password { get; set; }

    /// <summary>Database index. Default: 0.</summary>
    public int Database { get; set; }

    /// <summary>Enable SSL/TLS.</summary>
    public bool Ssl { get; set; }

    /// <summary>Connection pool size. Default: 10.</summary>
    public int PoolSize { get; set; } = 10;

    /// <summary>Connection and command timeout. Default: 5 seconds.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Maximum retry attempts. Default: 3.</summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>Base delay between retries. Default: 100ms.</summary>
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromMilliseconds(100);
}
