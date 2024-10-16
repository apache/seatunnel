/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.tencent.vectordb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.tencent.vectordb.exception.TencentVectorDBConnectorErrorCode;
import org.apache.seatunnel.connectors.tencent.vectordb.exception.TencentVectorDBConnectorException;
import org.apache.seatunnel.connectors.tencent.vectordb.utils.ConverterUtils;

import com.tencent.tcvectordb.client.RPCVectorDBClient;
import com.tencent.tcvectordb.client.VectorDBClient;
import com.tencent.tcvectordb.model.Collection;
import com.tencent.tcvectordb.model.Database;
import com.tencent.tcvectordb.model.Document;
import com.tencent.tcvectordb.model.param.database.ConnectParam;
import com.tencent.tcvectordb.model.param.dml.QueryParam;
import com.tencent.tcvectordb.model.param.enums.ReadConsistencyEnum;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.API_KEY;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.URL;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.USER_NAME;

@Slf4j
public class TencentVectorDBSourceReader
        implements SourceReader<SeaTunnelRow, TencentVectorDBSourceSplit> {
    private final Deque<TencentVectorDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final ReadonlyConfig config;
    private final Context context;
    private Map<TablePath, CatalogTable> sourceTables;
    private VectorDBClient client;
    private final AtomicLong offSet = new AtomicLong(0);

    private volatile boolean noMoreSplit;

    public TencentVectorDBSourceReader(
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
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withUrl(config.get(URL))
                        .withUsername(config.get(USER_NAME))
                        .withKey(config.get(API_KEY))
                        .withTimeout(30)
                        .build();
        client = new RPCVectorDBClient(connectParam, ReadConsistencyEnum.EVENTUAL_CONSISTENCY);
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
            TencentVectorDBSourceSplit split = pendingSplits.poll();
            if (null != split) {
                try {
                    log.info("Begin to read data from split: " + split);
                    TablePath tablePath = split.getTablePath();
                    TableSchema tableSchema = sourceTables.get(tablePath).getTableSchema();
                    log.info("begin to read data from pinecone, table schema: " + tableSchema);
                    if (null == tableSchema) {
                        throw new TencentVectorDBConnectorException(
                                TencentVectorDBConnectorErrorCode.SOURCE_TABLE_SCHEMA_IS_NULL);
                    }
                    Database database = client.database(tablePath.getDatabaseName());
                    Collection collection = database.collection(tablePath.getTableName());
                    while (true) {
                        QueryParam queryParam =
                                QueryParam.newBuilder()
                                        .withRetrieveVector(true)
                                        .withLimit((long) config.get(BATCH_SIZE))
                                        .withOffset(offSet.get())
                                        .build();
                        List<Document> documents = collection.query(queryParam);
                        if (documents.isEmpty()) {
                            break;
                        }
                        offSet.addAndGet(documents.size());
                        for (Document document : documents) {
                            SeaTunnelRow row =
                                    ConverterUtils.convertToSeatunnelRow(tableSchema, document);
                            row.setTableId(tablePath.getFullName());
                            output.collect(row);
                        }
                    }
                } catch (Exception e) {
                    log.error("Read data from split: " + split + " failed", e);
                    throw new TencentVectorDBConnectorException(
                            TencentVectorDBConnectorErrorCode.READ_DATA_FAIL, e);
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
    public List<TencentVectorDBSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    /**
     * Add the split checkpoint state to reader.
     *
     * @param splits split checkpoint state.
     */
    @Override
    public void addSplits(List<TencentVectorDBSourceSplit> splits) {
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
