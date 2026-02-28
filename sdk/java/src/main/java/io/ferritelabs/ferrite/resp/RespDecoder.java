package io.ferritelabs.ferrite.resp;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes RESP2 protocol responses.
 * Returns: String for simple/bulk strings, Long for integers,
 * List&lt;Object&gt; for arrays, null for null bulk strings.
 *
 * @throws FerriteProtocolException on RESP error responses or protocol violations
 */
public class RespDecoder {

    private final BufferedInputStream in;

    public RespDecoder(InputStream in) {
        this.in = new BufferedInputStream(in);
    }

    /**
     * Reads a single RESP value.
     */
    public Object readValue() throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new IOException("Connection closed");
        }
        String line = readLine();

        switch (type) {
            case '+': // Simple string
                return line;

            case '-': // Error
                throw new FerriteProtocolException(line);

            case ':': // Integer
                return Long.parseLong(line);

            case '$': // Bulk string
                int length = Integer.parseInt(line);
                if (length == -1) {
                    return null;
                }
                byte[] buf = new byte[length];
                int read = 0;
                while (read < length) {
                    int n = in.read(buf, read, length - read);
                    if (n == -1) {
                        throw new IOException("Unexpected end of stream");
                    }
                    read += n;
                }
                // Consume trailing \r\n
                in.read(); // \r
                in.read(); // \n
                return new String(buf, StandardCharsets.UTF_8);

            case '*': // Array
                int count = Integer.parseInt(line);
                if (count == -1) {
                    return null;
                }
                List<Object> array = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    array.add(readValue());
                }
                return array;

            default:
                throw new IOException("Unknown RESP type: " + (char) type);
        }
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int prev = 0;
        while (true) {
            int c = in.read();
            if (c == -1) {
                throw new IOException("Unexpected end of stream");
            }
            if (c == '\n' && prev == '\r') {
                sb.setLength(sb.length() - 1); // Remove \r
                return sb.toString();
            }
            sb.append((char) c);
            prev = c;
        }
    }
}
