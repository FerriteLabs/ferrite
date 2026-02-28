package io.ferritelabs.ferrite.resp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Encodes commands into RESP2 protocol format.
 */
public class RespEncoder {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);

    private final OutputStream out;

    public RespEncoder(OutputStream out) {
        this.out = out;
    }

    /**
     * Writes a RESP command as an array of bulk strings.
     */
    public void writeCommand(String... args) throws IOException {
        // Array header: *N\r\n
        out.write(('*' + Integer.toString(args.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (String arg : args) {
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            // Bulk string: $N\r\n<data>\r\n
            out.write(('$' + Integer.toString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(bytes);
            out.write(CRLF);
        }
        out.flush();
    }
}
