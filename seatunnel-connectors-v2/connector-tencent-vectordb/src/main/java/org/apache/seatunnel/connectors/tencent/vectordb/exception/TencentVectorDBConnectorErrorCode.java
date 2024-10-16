package org.apache.seatunnel.connectors.tencent.vectordb.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

import lombok.Getter;

@Getter
public enum TencentVectorDBConnectorErrorCode implements SeaTunnelErrorCode {
    SOURCE_TABLE_SCHEMA_IS_NULL("TC-VECTORDB-01", "Source table schema is null"),
    READ_DATA_FAIL("TC-VECTORDB-02", "Read data fail");

    private final String code;
    private final String description;

    TencentVectorDBConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
