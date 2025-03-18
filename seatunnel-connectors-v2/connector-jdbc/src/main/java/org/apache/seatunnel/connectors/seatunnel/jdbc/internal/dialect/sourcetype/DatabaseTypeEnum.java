package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sourcetype;

public enum DatabaseTypeEnum {
    MYSQL("MySQL"),
    ORACLE("Oracle"),
    SQLSERVER("SqlServer"),
    POSTGRESQL("Postgres");
    private final String value;

    DatabaseTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
