package org.apache.seatunnel.connectors.seatunnel.access.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum AccessConnectorErrorCode implements SeaTunnelErrorCode {
    FIELD_NOT_IN_TABLE("ACCESS-01", "Field is not existed in target table"),

    CLOSE_CQL_CONNECT_FAILED("ACCESS-03", "Close connect of access failed");
    private final String code;
    private final String description;

    AccessConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}
