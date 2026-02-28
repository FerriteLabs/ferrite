package io.ferritelabs.ferrite.resp;

/**
 * Exception thrown when the Ferrite server returns a RESP error response.
 */
public class FerriteProtocolException extends RuntimeException {

    public FerriteProtocolException(String message) {
        super(message);
    }
}
