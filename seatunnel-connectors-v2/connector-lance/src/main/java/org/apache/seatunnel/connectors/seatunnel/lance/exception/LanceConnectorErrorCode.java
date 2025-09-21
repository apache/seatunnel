package org.apache.seatunnel.connectors.seatunnel.lance.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum LanceConnectorErrorCode implements SeaTunnelErrorCode {
    TABLE_EXISTS_EXCEPTION("LANCE-01", "Table Exists response exception"),
    ;

    private final String code;
    private final String description;

    LanceConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    };

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
