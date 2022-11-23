package org.apache.seatunnel.connectors.seatunnel.myhours.source.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum MyHoursConnectorErrorCode implements SeaTunnelErrorCode {
    LOGIN_MYHOURS_EXCEPTION_WITH_RESPONCE("MYHOURS-01", "Login http client execute exception with reponce message"),
    LOGIN_MYHOURS_EXCEPTION_WITHOUT_RESPONCE("MYHOURS-02", "Login http client execute exception without reponce message");

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
