package org.apache.seatunnel.plugin;

/**
 * an Exception used for the scenes when plugin closed error.
 */
public class PluginClosedException extends RuntimeException {

    public PluginClosedException() {
    }

    public PluginClosedException(String message) {
        super(message);
    }

    public PluginClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public PluginClosedException(Throwable cause) {
        super(cause);
    }

    public PluginClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
