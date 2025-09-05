package org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.relational;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class IoTDBRelationalRecord {

    String tableName;
    Long timestamp;
    List<String> tags;
    List<String> attributes;
    List<Object> fields;

    @Override
    public String toString() {
        return String.format("tableName: %s; timestamp: %s", tableName, timestamp);
    }
}
