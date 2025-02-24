package org.apache.seatunnel.connectors.seatunnel.aerospike.config;

import java.io.Serializable;

public enum DataFormatType implements Serializable {
    MAP_FORMAT("map"), // 将数据解析为Map结构
    STRING_FORMAT("string"), // 直接使用字符串格式
    KEY_VALUE_FORMAT("kv"); // 每个字段作为单独的bin

    public final String dataFormatType;

    DataFormatType(String dataFormatType) {
        this.dataFormatType = dataFormatType;
    }
}
