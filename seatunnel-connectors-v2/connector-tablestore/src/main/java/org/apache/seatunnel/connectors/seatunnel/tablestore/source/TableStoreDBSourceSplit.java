package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class TableStoreDBSourceSplit implements SourceSplit {

    private Integer splitId;
    private String tableName;
    private String primaryKey;

    @Override
    public String splitId() {
        return splitId.toString();
    }
}
