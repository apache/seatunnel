package org.apache.seatunnel.connectors.pinecone.source;

import io.pinecone.proto.*;
import io.pinecone.proto.Vector;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.pinecone.exception.PineconeConnectionErrorCode;
import org.apache.seatunnel.connectors.pinecone.exception.PineconeConnectorException;
import org.apache.seatunnel.connectors.pinecone.utils.ConverterUtils;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig.API_KEY;
import static org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig.BATCH_SIZE;

@Slf4j
public class PineconeSourceReader implements SourceReader<SeaTunnelRow, PineconeSourceSplit> {
    private final Deque<PineconeSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final ReadonlyConfig config;
    private final Context context;
    private final Map<TablePath, CatalogTable> sourceTables;
    private Pinecone pinecone;
    private String paginationToken;

    private volatile boolean noMoreSplit;

    public PineconeSourceReader(
            Context readerContext,
            ReadonlyConfig config,
            Map<TablePath, CatalogTable> sourceTables) {
        this.context = readerContext;
        this.config = config;
        this.sourceTables = sourceTables;
    }

    /** Open the source reader. */
    @Override
    public void open() throws Exception {
        pinecone = new Pinecone.Builder(config.get(API_KEY)).build();
    }

    /**
     * Called to close the reader, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {}

    /**
     * Generate the next batch of records.
     *
     * @param output output collector.
     * @throws Exception if error occurs.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            PineconeSourceSplit split = pendingSplits.poll();
            if (null != split) {
                try {
                    log.info("Begin to read data from split: " + split);
                    TablePath tablePath = split.getTablePath();
                    String namespace = split.getNamespace();
                    TableSchema tableSchema = sourceTables.get(tablePath).getTableSchema();
                    log.info("begin to read data from pinecone, table schema: " + tableSchema);
                    if (null == tableSchema) {
                        throw new PineconeConnectorException(
                                PineconeConnectionErrorCode.SOURCE_TABLE_SCHEMA_IS_NULL);
                    }
                    Index index = pinecone.getIndexConnection(tablePath.getTableName());
                    ListResponse listResponse;
                    while (!(Objects.equals(paginationToken, ""))) {
                        if (paginationToken == null) {
                            listResponse = index.list(namespace, config.get(BATCH_SIZE));
                        } else {
                            listResponse =
                                    index.list(namespace, config.get(BATCH_SIZE), paginationToken);
                        }
                        List<ListItem> vectorsList = listResponse.getVectorsList();
                        List<String> ids =
                                vectorsList.stream()
                                        .map(ListItem::getId)
                                        .collect(Collectors.toList());
                        if (ids.isEmpty()) {
                            break;
                        }
                        FetchResponse fetchResponse = index.fetch(ids, namespace);
                        Map<String, Vector> vectorMap = fetchResponse.getVectorsMap();
                        for (Map.Entry<String, Vector> entry : vectorMap.entrySet()) {
                            Vector vector = entry.getValue();
                            SeaTunnelRow row =
                                    ConverterUtils.convertToSeatunnelRow(tableSchema, vector);
                            row.setPartitionName(namespace);
                            row.setTableId(tablePath.getFullName());
                            output.collect(row);
                        }
                        Pagination pagination = listResponse.getPagination();
                        paginationToken = pagination.getNext();
                    }
                } catch (Exception e) {
                    log.error("Read data from split: " + split + " failed", e);
                    throw new PineconeConnectorException(
                            PineconeConnectionErrorCode.READ_DATA_FAIL, e);
                }
            } else {
                if (!noMoreSplit) {
                    log.info("Pinecone source wait split!");
                }
            }
        }
        if (noMoreSplit
                && pendingSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded pinecone source");
            context.signalNoMoreElement();
        }
        Thread.sleep(1000L);
    }

    /**
     * Get the current split checkpoint state by checkpointId.
     *
     * <p>If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId checkpoint Id.
     * @return split checkpoint state.
     * @throws Exception if error occurs.
     */
    @Override
    public List<PineconeSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    /**
     * Add the split checkpoint state to reader.
     *
     * @param splits split checkpoint state.
     */
    @Override
    public void addSplits(List<PineconeSourceSplit> splits) {
        log.info("Adding pinecone splits to reader: " + splits);
        pendingSplits.addAll(splits);
    }

    /**
     * This method is called when the reader is notified that it will not receive any further
     * splits.
     *
     * <p>It is triggered when the enumerator calls {@link
     * SourceSplitEnumerator.Context#signalNoMoreSplits(int)} with the reader's parallel subtask.
     */
    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this milvus reader will not add new split.");
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
