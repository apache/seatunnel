package org.apache.seatunnel.connectors.seatunnel.typesense.dto;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig;

import lombok.Data;

@Data
public class CollectionInfo {

    private String collection;
    private String type;
    private String[] primaryKeys;
    private String keyDelimiter;

    public CollectionInfo(String collection, ReadonlyConfig config) {
        this.collection = collection;
        if (config.getOptional(SinkConfig.PRIMARY_KEYS).isPresent()) {
            primaryKeys = config.get(SinkConfig.PRIMARY_KEYS).toArray(new String[0]);
        }
        keyDelimiter = config.get(SinkConfig.KEY_DELIMITER);
    }
}
