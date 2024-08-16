package org.apache.seatunnel.connectors.seatunnel.typesense.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseClient;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.CollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.TypesenseRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.state.TypesenseCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.state.TypesenseSinkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class TypesenseSinkWriter implements SinkWriter<SeaTunnelRow,
        TypesenseCommitInfo,
        TypesenseSinkState>,
        SupportMultiTableSinkWriter<Void> {

    private final Context context;
    private final int maxBatchSize;
    private final SeaTunnelRowSerializer seaTunnelRowSerializer;

    // 存储的是请求ES的JSON参数
    private final List<String> requestEsList;
    private TypesenseClient typesenseClient;
    private RetryMaterial retryMaterial;
    private static final long DEFAULT_SLEEP_TIME_MS = 200L;

    public TypesenseSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            int maxBatchSize,
            int maxRetryCount) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;


        CollectionInfo collectionInfo = new CollectionInfo(catalogTable.getTableId().getTableName(), config);
        typesenseClient = TypesenseClient.createInstance(config);
        this.seaTunnelRowSerializer =
                new TypesenseRowSerializer(
                        collectionInfo,
                        catalogTable.getSeaTunnelRowType());

        this.requestEsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial =
                new RetryMaterial(maxRetryCount, true, exception -> true, DEFAULT_SLEEP_TIME_MS);
    }


    @Override
    public void write(SeaTunnelRow element) {
        if (RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }

        String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
        requestEsList.add(indexRequestRow);
        if (requestEsList.size() >= maxBatchSize) {
            //TODO 实际批量写入
        }
    }

    @Override
    public Optional<TypesenseCommitInfo> prepareCommit() {
        //TODO 实际批量写入
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        //TODO 实际批量写入
    }

}
