package org.apache.seatunnel.azuredataexplorer.exception;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class AzureDataExplorerConnectorException extends SeaTunnelRuntimeException {

    public AzureDataExplorerConnectorException(AzureDataExplorerErrorCode code, String message) {
        super(code, message);
    }

    public AzureDataExplorerConnectorException(
            AzureDataExplorerErrorCode code, String message, Throwable cause) {
        super(code, message, cause);
    }
}
