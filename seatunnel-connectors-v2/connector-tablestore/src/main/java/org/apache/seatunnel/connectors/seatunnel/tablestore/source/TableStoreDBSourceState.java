package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
public class TableStoreDBSourceState implements Serializable {

    private boolean shouldEnumerate;
    private Map<Integer, List<TableStoreDBSourceSplit>> pendingSplits;
}
