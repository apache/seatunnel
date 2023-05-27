package org.apache.seatunnel.connectors.seatunnel.neo4j.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum Neo4jConnectorErrorCode implements SeaTunnelErrorCode {
    DATE_BASE_ERROR("NEO4J-01", "Neo4j Database Error");
    private final String code;
    private final String description;

    Neo4jConnectorErrorCode(String code, String description) {
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
