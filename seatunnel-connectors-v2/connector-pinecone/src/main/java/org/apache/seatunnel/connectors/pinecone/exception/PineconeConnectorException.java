package org.apache.seatunnel.connectors.pinecone.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class PineconeConnectorException extends SeaTunnelRuntimeException {
    public PineconeConnectorException(SeaTunnelErrorCode seaTunnelErrorCode) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage());
    }

    public PineconeConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage(), cause);
    }
}
