package org.apache.seatunnel.connectors.seatunnel.myhours.source.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum MyHoursConnectorErrorCode implements SeaTunnelErrorCode {
    LOGIN_MYHOURS_EXCEPTION("MYHOURS-01", "Login http client execute exception");

    private final String code;

    private final String description;

    MyHoursConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getErrorMessage() {
        return SeaTunnelErrorCode.super.getErrorMessage();
    }
}
