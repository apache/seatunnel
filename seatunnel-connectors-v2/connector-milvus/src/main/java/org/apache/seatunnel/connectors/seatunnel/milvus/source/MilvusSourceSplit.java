package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MilvusSourceSplit implements SourceSplit {

    private TablePath tablePath;
    private String splitId;
    private String partitionName;

    @Override
    public String splitId() {
        return splitId;
    }
}
