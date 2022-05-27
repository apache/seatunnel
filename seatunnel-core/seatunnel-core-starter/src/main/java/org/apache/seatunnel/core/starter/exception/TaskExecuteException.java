package org.apache.seatunnel.core.starter.exception;

public class TaskExecuteException extends Exception {

    public TaskExecuteException(String message) {
        super(message);
    }

    public TaskExecuteException(String message, Throwable cause) {
        super(message, cause);
    }
}
