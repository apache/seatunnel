package org.apache.seatunnel.azuredataexplorer.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum AzureDataExplorerErrorCode implements SeaTunnelErrorCode {
    INGESTION_FAILED("ADX-01", "Failed to ingest data into Azure Data Explorer"),
    QUERY_FAILED("ADX-02", "Failed to execute KQL query against Azure Data Explorer"),
    CONNECTION_FAILED("ADX-03", "Failed to connect to Azure Data Explorer cluster"),
    SERIALIZATION_FAILED("ADX-04", "Failed to serialize SeaTunnelRow to CSV"),
    UNSUPPORTED_DATA_TYPE("ADX-05", "Unsupported SeaTunnel data type for ADX connector");

    private final String code;
    private final String description;

    AzureDataExplorerErrorCode(String code, String description) {
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
