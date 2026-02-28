package io.ferritelabs.ferrite;

import java.time.Duration;
import javax.net.ssl.SSLContext;

/**
 * Configuration for a Ferrite client connection.
 * Use the {@link Builder} to construct instances.
 */
public class FerriteConfig {

    private final String host;
    private final int port;
    private final String password;
    private final int database;
    private final boolean ssl;
    private final SSLContext sslContext;
    private final int poolSize;
    private final Duration timeout;
    private final int maxRetries;
    private final Duration retryBackoff;

    private FerriteConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.password = builder.password;
        this.database = builder.database;
        this.ssl = builder.ssl;
        this.sslContext = builder.sslContext;
        this.poolSize = builder.poolSize;
        this.timeout = builder.timeout;
        this.maxRetries = builder.maxRetries;
        this.retryBackoff = builder.retryBackoff;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getPassword() { return password; }
    public int getDatabase() { return database; }
    public boolean isSsl() { return ssl; }
    public SSLContext getSslContext() { return sslContext; }
    public int getPoolSize() { return poolSize; }
    public Duration getTimeout() { return timeout; }
    public int getMaxRetries() { return maxRetries; }
    public Duration getRetryBackoff() { return retryBackoff; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 6379;
        private String password;
        private int database = 0;
        private boolean ssl = false;
        private SSLContext sslContext;
        private int poolSize = 10;
        private Duration timeout = Duration.ofSeconds(5);
        private int maxRetries = 3;
        private Duration retryBackoff = Duration.ofMillis(100);

        public Builder host(String host) { this.host = host; return this; }
        public Builder port(int port) { this.port = port; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder database(int database) { this.database = database; return this; }
        public Builder ssl(boolean ssl) { this.ssl = ssl; return this; }
        public Builder sslContext(SSLContext sslContext) { this.sslContext = sslContext; return this; }
        public Builder poolSize(int poolSize) { this.poolSize = poolSize; return this; }
        public Builder timeout(Duration timeout) { this.timeout = timeout; return this; }
        public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder retryBackoff(Duration retryBackoff) { this.retryBackoff = retryBackoff; return this; }

        public FerriteConfig build() {
            return new FerriteConfig(this);
        }
    }
}
