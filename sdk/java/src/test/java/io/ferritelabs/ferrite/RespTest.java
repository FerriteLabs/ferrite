package io.ferritelabs.ferrite;

import io.ferritelabs.ferrite.resp.RespEncoder;
import io.ferritelabs.ferrite.resp.RespDecoder;
import io.ferritelabs.ferrite.resp.FerriteProtocolException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Simple test runner for Ferrite Java SDK — no JUnit dependency required.
 */
public class RespTest {

    private static int passed = 0;
    private static int failed = 0;

    private static void assertCondition(boolean condition, String name) {
        if (condition) {
            passed++;
            System.out.println("  \u2713 " + name);
        } else {
            failed++;
            System.out.println("  \u2717 " + name);
        }
    }

    private static void assertEquals(Object expected, Object actual, String name) {
        assertCondition(expected.equals(actual),
            name + " (expected=" + expected + ", actual=" + actual + ")");
    }

    private static void assertNull(Object value, String name) {
        assertCondition(value == null, name + " (expected=null, actual=" + value + ")");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Ferrite Java SDK Tests");
        System.out.println("======================");

        System.out.println("\nRESP Encoder:");
        testEncodeCommand();
        testEncodeGetCommand();
        testEncodePingCommand();

        System.out.println("\nRESP Decoder:");
        testDecodeSimpleString();
        testDecodeError();
        testDecodeInteger();
        testDecodeBulkString();
        testDecodeNull();
        testDecodeArray();

        System.out.println("\nConfig Builder:");
        testConfigDefaults();
        testConfigCustom();

        System.out.println("\n" + passed + " passed, " + failed + " failed");
        System.exit(failed > 0 ? 1 : 0);
    }

    // --- Encoder Tests ---

    static void testEncodeCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespEncoder encoder = new RespEncoder(out);
        encoder.writeCommand("SET", "key", "value");
        String result = out.toString(StandardCharsets.US_ASCII);
        assertEquals("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", result,
            "encode SET key value");
    }

    static void testEncodeGetCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespEncoder encoder = new RespEncoder(out);
        encoder.writeCommand("GET", "mykey");
        String result = out.toString(StandardCharsets.US_ASCII);
        assertEquals("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n", result,
            "encode GET mykey");
    }

    static void testEncodePingCommand() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespEncoder encoder = new RespEncoder(out);
        encoder.writeCommand("PING");
        String result = out.toString(StandardCharsets.US_ASCII);
        assertEquals("*1\r\n$4\r\nPING\r\n", result,
            "encode PING");
    }

    // --- Decoder Tests ---

    static void testDecodeSimpleString() throws Exception {
        RespDecoder decoder = decoderFor("+OK\r\n");
        Object value = decoder.readValue();
        assertEquals("OK", value, "decode simple string +OK");
    }

    static void testDecodeError() throws Exception {
        RespDecoder decoder = decoderFor("-ERR message\r\n");
        try {
            decoder.readValue();
            assertCondition(false, "decode error should throw");
        } catch (FerriteProtocolException e) {
            assertEquals("ERR message", e.getMessage(), "decode error message");
        }
    }

    static void testDecodeInteger() throws Exception {
        RespDecoder decoder = decoderFor(":1000\r\n");
        Object value = decoder.readValue();
        assertEquals(1000L, value, "decode integer :1000");
    }

    static void testDecodeBulkString() throws Exception {
        RespDecoder decoder = decoderFor("$5\r\nhello\r\n");
        Object value = decoder.readValue();
        assertEquals("hello", value, "decode bulk string hello");
    }

    static void testDecodeNull() throws Exception {
        RespDecoder decoder = decoderFor("$-1\r\n");
        Object value = decoder.readValue();
        assertNull(value, "decode null bulk string");
    }

    @SuppressWarnings("unchecked")
    static void testDecodeArray() throws Exception {
        RespDecoder decoder = decoderFor("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        Object value = decoder.readValue();
        assertCondition(value instanceof List, "decode array is List");
        List<Object> list = (List<Object>) value;
        assertEquals(2, list.size(), "decode array length");
        assertEquals("foo", list.get(0), "decode array[0]");
        assertEquals("bar", list.get(1), "decode array[1]");
    }

    // --- Config Tests ---

    static void testConfigDefaults() {
        FerriteConfig config = FerriteConfig.builder().build();
        assertEquals("localhost", config.getHost(), "default host");
        assertEquals(6379, config.getPort(), "default port");
        assertNull(config.getPassword(), "default password");
        assertEquals(0, config.getDatabase(), "default database");
        assertCondition(!config.isSsl(), "default ssl=false");
        assertEquals(10, config.getPoolSize(), "default poolSize");
        assertEquals(Duration.ofSeconds(5), config.getTimeout(), "default timeout");
        assertEquals(3, config.getMaxRetries(), "default maxRetries");
        assertEquals(Duration.ofMillis(100), config.getRetryBackoff(), "default retryBackoff");
    }

    static void testConfigCustom() {
        FerriteConfig config = FerriteConfig.builder()
            .host("ferrite.example.com")
            .port(6380)
            .password("secret")
            .database(2)
            .ssl(true)
            .poolSize(20)
            .timeout(Duration.ofSeconds(10))
            .maxRetries(5)
            .retryBackoff(Duration.ofMillis(200))
            .build();

        assertEquals("ferrite.example.com", config.getHost(), "custom host");
        assertEquals(6380, config.getPort(), "custom port");
        assertEquals("secret", config.getPassword(), "custom password");
        assertEquals(2, config.getDatabase(), "custom database");
        assertCondition(config.isSsl(), "custom ssl=true");
        assertEquals(20, config.getPoolSize(), "custom poolSize");
        assertEquals(Duration.ofSeconds(10), config.getTimeout(), "custom timeout");
        assertEquals(5, config.getMaxRetries(), "custom maxRetries");
        assertEquals(Duration.ofMillis(200), config.getRetryBackoff(), "custom retryBackoff");
    }

    // --- Helpers ---

    private static RespDecoder decoderFor(String data) {
        return new RespDecoder(new ByteArrayInputStream(data.getBytes(StandardCharsets.US_ASCII)));
    }
}
