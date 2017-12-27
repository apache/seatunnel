package io.github.interestinglab.waterdrop.docutils;

/**
 * Created by gaoyingju on 18/09/2017.
 */
public class PluginDocRuntimeException extends RuntimeException {

    public PluginDocRuntimeException() {
        super();
    }

    public PluginDocRuntimeException(String message) {
        super(message);
    }

    public PluginDocRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public PluginDocRuntimeException(Throwable cause) {
        super(cause);
    }
}
