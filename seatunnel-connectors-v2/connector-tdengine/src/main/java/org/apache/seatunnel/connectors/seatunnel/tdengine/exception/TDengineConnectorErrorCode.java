package org.apache.seatunnel.connectors.seatunnel.tdengine.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum TDengineConnectorErrorCode implements SeaTunnelErrorCode {
    SQL_OPERATION_FAILED("TDengine-01", "execute sql failed"),
    CONNECTION_FAILED("TDengine-02", "connection operation failed"),
    TYPE_MAPPER_FAILED("TDengine-03", "type mapping failed"),
    READER_FAILED("TDengine-04", "reader operation failed"),
    WRITER_FAILED("TDengine-05", "writer operation failed");

    private final String code;
    private final String description;

    TDengineConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
