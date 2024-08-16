package org.apache.seatunnel.connectors.seatunnel.typesense.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum TypesenseConnectorErrorCode implements SeaTunnelErrorCode {

    QUERY_PARAM_ERROR("TYPESENSE-01", "Query parameter error");
    private final String code;
    private final String description;

    TypesenseConnectorErrorCode(String code, String description) {
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
