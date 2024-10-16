package org.apache.seatunnel.connectors.tencent.vectordb.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class TencentVectorDBConnectorException extends SeaTunnelRuntimeException {
    public TencentVectorDBConnectorException(SeaTunnelErrorCode seaTunnelErrorCode) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage());
    }

    public TencentVectorDBConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage(), cause);
    }
}
