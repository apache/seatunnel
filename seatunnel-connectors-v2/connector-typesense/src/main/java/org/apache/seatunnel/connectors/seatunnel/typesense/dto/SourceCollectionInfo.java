package org.apache.seatunnel.connectors.seatunnel.typesense.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class SourceCollectionInfo implements Serializable {
    private String collection;
    private String query;
    private long found;
    private int offset;
}
