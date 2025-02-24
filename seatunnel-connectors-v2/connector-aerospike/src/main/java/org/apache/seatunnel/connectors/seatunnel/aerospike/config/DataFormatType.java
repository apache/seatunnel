package org.apache.seatunnel.connectors.seatunnel.aerospike.config;

import lombok.Getter;

import java.io.Serializable;

@Getter
public enum DataFormatType implements Serializable {
    MAP("map"),
    STRING("string"),
    KEY_VALUE("kv");

    private final String format;

    DataFormatType(String format) {
        this.format = format;
    }

    public static DataFormatType fromString(String format) {
        for (DataFormatType type : DataFormatType.values()) {
            if (type.format.equalsIgnoreCase(format)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown format type: " + format);
    }

}
