package org.apache.seatunnel.connectors.seatunnel.aerospike.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class AerospikeConnectorException extends SeaTunnelRuntimeException {
    public AerospikeConnectorException(SeaTunnelErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
    }

    public AerospikeConnectorException(
            SeaTunnelErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode, errorMessage, cause);
    }
}
