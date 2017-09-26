package org.interestinglab.waterdrop.config;

/**
 * Created by gaoyingju on 18/09/2017.
 */
public class ConfigRuntimeException extends RuntimeException {

    public ConfigRuntimeException() {
        super();
    }

    public ConfigRuntimeException(String message) {
        super(message);
    }

    public ConfigRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigRuntimeException(Throwable cause) {
        super(cause);
    }
}
