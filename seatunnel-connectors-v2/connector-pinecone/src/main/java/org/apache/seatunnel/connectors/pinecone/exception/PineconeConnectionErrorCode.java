package org.apache.seatunnel.connectors.pinecone.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

import lombok.Getter;

@Getter
public enum PineconeConnectionErrorCode implements SeaTunnelErrorCode {
    SOURCE_TABLE_SCHEMA_IS_NULL("PINECONE-01", "Source table schema is null"),
    READ_DATA_FAIL("PINECONE-02", "Read data fail");

    private final String code;
    private final String description;

    PineconeConnectionErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
