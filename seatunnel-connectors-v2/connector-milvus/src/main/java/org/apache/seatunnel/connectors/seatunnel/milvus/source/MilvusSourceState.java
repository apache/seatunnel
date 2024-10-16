package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class MilvusSourceState implements Serializable {
    private List<TablePath> pendingTables;
    private Map<Integer, List<MilvusSourceSplit>> pendingSplits;
}
