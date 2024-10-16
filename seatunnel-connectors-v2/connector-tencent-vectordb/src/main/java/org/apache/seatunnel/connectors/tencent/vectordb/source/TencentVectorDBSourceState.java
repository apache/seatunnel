package org.apache.seatunnel.connectors.tencent.vectordb.source;

import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class TencentVectorDBSourceState implements Serializable {
    private List<TablePath> pendingTables;
    private Map<Integer, List<TencentVectorDBSourceSplit>> pendingSplits;
}
