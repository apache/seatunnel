package io.github.interestinglab.waterdrop.common.config;

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
