package io.ferritelabs.ferrite.pool;

import io.ferritelabs.ferrite.FerriteConfig;
import io.ferritelabs.ferrite.resp.RespDecoder;
import io.ferritelabs.ferrite.resp.RespEncoder;

import javax.net.ssl.SSLSocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple connection pool for Ferrite connections.
 */
public class ConnectionPool implements Closeable {

    private final FerriteConfig config;
    private final BlockingQueue<Connection> idle;
    private final AtomicInteger size;
    private final AtomicBoolean closed;

    public ConnectionPool(FerriteConfig config) {
        this.config = config;
        this.idle = new ArrayBlockingQueue<>(config.getPoolSize());
        this.size = new AtomicInteger(0);
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Gets a connection from the pool, creating one if needed.
     */
    public Connection get() throws IOException {
        if (closed.get()) {
            throw new IOException("Pool is closed");
        }

        // Try to get an idle connection
        Connection conn = idle.poll();
        if (conn != null) {
            return conn;
        }

        // Create a new one if under the limit
        if (size.get() < config.getPoolSize()) {
            size.incrementAndGet();
            try {
                return createConnection();
            } catch (IOException e) {
                size.decrementAndGet();
                throw e;
            }
        }

        // Wait for an available connection
        try {
            conn = idle.take();
            return conn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for connection", e);
        }
    }

    /**
     * Returns a connection to the pool.
     */
    public void release(Connection conn) {
        if (closed.get() || !idle.offer(conn)) {
            closeQuietly(conn);
            size.decrementAndGet();
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Connection conn;
        while ((conn = idle.poll()) != null) {
            closeQuietly(conn);
        }
    }

    private Connection createConnection() throws IOException {
        Socket socket;
        if (config.isSsl()) {
            SSLSocketFactory factory = config.getSslContext() != null
                    ? config.getSslContext().getSocketFactory()
                    : (SSLSocketFactory) SSLSocketFactory.getDefault();
            socket = factory.createSocket();
        } else {
            socket = new Socket();
        }

        int timeoutMs = (int) config.getTimeout().toMillis();
        socket.connect(new InetSocketAddress(config.getHost(), config.getPort()), timeoutMs);
        socket.setSoTimeout(timeoutMs);

        return new Connection(socket);
    }

    private static void closeQuietly(Connection conn) {
        try {
            conn.close();
        } catch (IOException ignored) {
        }
    }

    /**
     * Represents a single connection to a Ferrite server.
     */
    public static class Connection implements Closeable {
        private final Socket socket;
        private final RespEncoder encoder;
        private final RespDecoder decoder;

        Connection(Socket socket) throws IOException {
            this.socket = socket;
            this.encoder = new RespEncoder(socket.getOutputStream());
            this.decoder = new RespDecoder(socket.getInputStream());
        }

        public RespEncoder getEncoder() { return encoder; }
        public RespDecoder getDecoder() { return decoder; }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
