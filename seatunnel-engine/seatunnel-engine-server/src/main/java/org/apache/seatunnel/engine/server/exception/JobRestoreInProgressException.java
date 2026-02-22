package org.apache.seatunnel.engine.server.exception;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;

public class JobRestoreInProgressException extends SeaTunnelEngineException {
    public JobRestoreInProgressException(String message) {
        super(message);
    }

    public JobRestoreInProgressException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public Throwable createException(String s, Throwable throwable) {
        return new JobRestoreInProgressException(s, throwable);
    }
}
