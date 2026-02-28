using System.Text;
using Ferrite.Client;
using Ferrite.Client.Protocol;

/// <summary>
/// Simple test runner for Ferrite .NET SDK — no xUnit dependency required.
/// </summary>
class Program
{
    private static int _passed;
    private static int _failed;

    static void Assert(bool condition, string name)
    {
        if (condition)
        {
            _passed++;
            Console.WriteLine($"  \u2713 {name}");
        }
        else
        {
            _failed++;
            Console.WriteLine($"  \u2717 {name}");
        }
    }

    static void AssertEqual<T>(T expected, T actual, string name) where T : notnull
    {
        Assert(expected.Equals(actual),
            $"{name} (expected={expected}, actual={actual})");
    }

    static void AssertNull(object? value, string name)
    {
        Assert(value is null, $"{name} (expected=null, actual={value})");
    }

    static async Task<int> Main()
    {
        Console.WriteLine("Ferrite .NET SDK Tests");
        Console.WriteLine("======================");

        Console.WriteLine("\nRESP Encoder:");
        await TestEncodeSetCommand();
        await TestEncodeGetCommand();
        await TestEncodePingCommand();

        Console.WriteLine("\nRESP Decoder:");
        await TestDecodeSimpleString();
        await TestDecodeError();
        await TestDecodeInteger();
        await TestDecodeBulkString();
        await TestDecodeNull();
        await TestDecodeArray();

        Console.WriteLine("\nOptions:");
        TestOptionsDefaults();
        TestOptionsCustom();

        Console.WriteLine($"\n{_passed} passed, {_failed} failed");
        return _failed > 0 ? 1 : 0;
    }

    // --- Encoder Tests ---

    static async Task TestEncodeSetCommand()
    {
        var ms = new MemoryStream();
        await RespProtocol.WriteCommandAsync(ms, ["SET", "key", "value"]);
        var result = Encoding.UTF8.GetString(ms.ToArray());
        AssertEqual("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", result,
            "encode SET key value");
    }

    static async Task TestEncodeGetCommand()
    {
        var ms = new MemoryStream();
        await RespProtocol.WriteCommandAsync(ms, ["GET", "mykey"]);
        var result = Encoding.UTF8.GetString(ms.ToArray());
        AssertEqual("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n", result,
            "encode GET mykey");
    }

    static async Task TestEncodePingCommand()
    {
        var ms = new MemoryStream();
        await RespProtocol.WriteCommandAsync(ms, ["PING"]);
        var result = Encoding.UTF8.GetString(ms.ToArray());
        AssertEqual("*1\r\n$4\r\nPING\r\n", result,
            "encode PING");
    }

    // --- Decoder Tests ---

    static async Task TestDecodeSimpleString()
    {
        var value = await DecodeValue("+OK\r\n");
        AssertEqual("OK", (string)value!, "decode simple string +OK");
    }

    static async Task TestDecodeError()
    {
        try
        {
            await DecodeValue("-ERR message\r\n");
            Assert(false, "decode error should throw");
        }
        catch (FerriteException ex)
        {
            AssertEqual("ERR message", ex.Message, "decode error message");
        }
    }

    static async Task TestDecodeInteger()
    {
        var value = await DecodeValue(":42\r\n");
        AssertEqual(42L, (long)value!, "decode integer :42");
    }

    static async Task TestDecodeBulkString()
    {
        var value = await DecodeValue("$5\r\nhello\r\n");
        AssertEqual("hello", (string)value!, "decode bulk string hello");
    }

    static async Task TestDecodeNull()
    {
        var value = await DecodeValue("$-1\r\n");
        AssertNull(value, "decode null bulk string");
    }

    static async Task TestDecodeArray()
    {
        var value = await DecodeValue("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        Assert(value is object?[], "decode array is object[]");
        var arr = (object?[])value!;
        AssertEqual(2, arr.Length, "decode array length");
        AssertEqual("foo", (string)arr[0]!, "decode array[0]");
        AssertEqual("bar", (string)arr[1]!, "decode array[1]");
    }

    // --- Options Tests ---

    static void TestOptionsDefaults()
    {
        var opts = new FerriteOptions();
        AssertEqual("localhost", opts.Host, "default host");
        AssertEqual(6379, opts.Port, "default port");
        AssertNull(opts.Password, "default password");
        AssertEqual(0, opts.Database, "default database");
        Assert(!opts.Ssl, "default ssl=false");
        AssertEqual(10, opts.PoolSize, "default poolSize");
        AssertEqual(TimeSpan.FromSeconds(5), opts.Timeout, "default timeout");
        AssertEqual(3, opts.MaxRetries, "default maxRetries");
        AssertEqual(TimeSpan.FromMilliseconds(100), opts.RetryBackoff, "default retryBackoff");
    }

    static void TestOptionsCustom()
    {
        var opts = new FerriteOptions
        {
            Host = "ferrite.example.com",
            Port = 6380,
            Password = "secret",
            Database = 2,
            Ssl = true,
            PoolSize = 20,
            Timeout = TimeSpan.FromSeconds(10),
            MaxRetries = 5,
            RetryBackoff = TimeSpan.FromMilliseconds(200)
        };

        AssertEqual("ferrite.example.com", opts.Host, "custom host");
        AssertEqual(6380, opts.Port, "custom port");
        AssertEqual("secret", opts.Password!, "custom password");
        AssertEqual(2, opts.Database, "custom database");
        Assert(opts.Ssl, "custom ssl=true");
        AssertEqual(20, opts.PoolSize, "custom poolSize");
        AssertEqual(TimeSpan.FromSeconds(10), opts.Timeout, "custom timeout");
        AssertEqual(5, opts.MaxRetries, "custom maxRetries");
        AssertEqual(TimeSpan.FromMilliseconds(200), opts.RetryBackoff, "custom retryBackoff");
    }

    // --- Helpers ---

    static async Task<object?> DecodeValue(string data)
    {
        var ms = new MemoryStream(Encoding.UTF8.GetBytes(data));
        var reader = new StreamReader(ms, Encoding.UTF8);
        return await RespProtocol.ReadValueAsync(reader);
    }
}
