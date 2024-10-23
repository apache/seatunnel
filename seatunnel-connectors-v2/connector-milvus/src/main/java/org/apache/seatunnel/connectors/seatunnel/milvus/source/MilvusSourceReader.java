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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.utils.source.MilvusSourceConverter;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import org.codehaus.plexus.util.StringUtils;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.GetLoadStateResponse;
import io.milvus.grpc.LoadState;
import io.milvus.grpc.QueryResults;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.AlterCollectionParam;
import io.milvus.param.collection.GetLoadStateParam;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.response.QueryResultsWrapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig.RATE_LIMIT;

@Slf4j
public class MilvusSourceReader implements SourceReader<SeaTunnelRow, MilvusSourceSplit> {

    private final Deque<MilvusSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final ReadonlyConfig config;
    private final Context context;
    private final Map<TablePath, CatalogTable> sourceTables;

    private MilvusServiceClient client;

    private volatile boolean noMoreSplit;

    public MilvusSourceReader(
            Context readerContext,
            ReadonlyConfig config,
            Map<TablePath, CatalogTable> sourceTables) {
        this.context = readerContext;
        this.config = config;
        this.sourceTables = sourceTables;
    }

    @Override
    public void open() throws Exception {
        client =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withUri(config.get(MilvusSourceConfig.URL))
                                .withToken(config.get(MilvusSourceConfig.TOKEN))
                                .build());
        setRateLimit(config.get(RATE_LIMIT).toString());
    }

    private void setRateLimit(String rateLimit) {
        log.info("Set rate limit: " + rateLimit);
        for (Map.Entry<TablePath, CatalogTable> entry : sourceTables.entrySet()) {
            TablePath tablePath = entry.getKey();
            String collectionName = tablePath.getTableName();

            AlterCollectionParam alterCollectionParam =
                    AlterCollectionParam.newBuilder()
                            .withDatabaseName(tablePath.getDatabaseName())
                            .withCollectionName(collectionName)
                            .withProperty("collection.queryRate.max.qps", rateLimit)
                            .build();
            R<RpcStatus> response = client.alterCollection(alterCollectionParam);
            if (response.getStatus() != R.Status.Success.getCode()) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.SERVER_RESPONSE_FAILED, response.getException());
            }
        }
        log.info("Set rate limit success");
    }

    @Override
    public void close() throws IOException {
        log.info("Close milvus source reader");
        setRateLimit("-1");
        client.close();
        log.info("Close milvus source reader success");
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            MilvusSourceSplit split = pendingSplits.poll();
            if (null != split) {
                try {
                    log.info("Begin to read data from split: " + split);
                    pollNextData(split, output);
                } catch (Exception e) {
                    log.error("Read data from split: " + split + " failed", e);
                    throw new MilvusConnectorException(MilvusConnectionErrorCode.READ_DATA_FAIL, e);
                }
            } else {
                if (!noMoreSplit) {
                    log.info("Milvus source wait split!");
                }
            }
        }
        if (noMoreSplit
                && pendingSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded milvus source");
            context.signalNoMoreElement();
        }
        Thread.sleep(1000L);
    }

    private void pollNextData(MilvusSourceSplit split, Collector<SeaTunnelRow> output)
            throws InterruptedException {
        TablePath tablePath = split.getTablePath();
        String partitionName = split.getPartitionName();
        TableSchema tableSchema = sourceTables.get(tablePath).getTableSchema();
        log.info("begin to read data from milvus, table schema: " + tableSchema);
        if (null == tableSchema) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.SOURCE_TABLE_SCHEMA_IS_NULL);
        }

        R<GetLoadStateResponse> loadStateResponse =
                client.getLoadState(
                        GetLoadStateParam.newBuilder()
                                .withDatabaseName(tablePath.getDatabaseName())
                                .withCollectionName(tablePath.getTableName())
                                .build());
        if (loadStateResponse.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.SERVER_RESPONSE_FAILED,
                    loadStateResponse.getException());
        }

        if (!LoadState.LoadStateLoaded.equals(loadStateResponse.getData().getState())) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.COLLECTION_NOT_LOADED);
        }
        QueryParam.Builder queryParam =
                QueryParam.newBuilder()
                        .withDatabaseName(tablePath.getDatabaseName())
                        .withCollectionName(tablePath.getTableName())
                        .withExpr("")
                        .withOutFields(Lists.newArrayList("count(*)"));

        if (StringUtils.isNotEmpty(partitionName)) {
            queryParam.withPartitionNames(Collections.singletonList(partitionName));
        }

        R<QueryResults> queryResultsR = client.query(queryParam.build());

        if (queryResultsR.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.SERVER_RESPONSE_FAILED,
                    loadStateResponse.getException());
        }
        QueryResultsWrapper wrapper = new QueryResultsWrapper(queryResultsR.getData());
        List<QueryResultsWrapper.RowRecord> records = wrapper.getRowRecords();
        log.info("Total records num: " + records.get(0).getFieldValues().get("count(*)"));

        long batchSize = (long) config.get(BATCH_SIZE);
        queryIteratorData(tablePath, partitionName, tableSchema, output, batchSize);
    }

    private void queryIteratorData(
            TablePath tablePath,
            String partitionName,
            TableSchema tableSchema,
            Collector<SeaTunnelRow> output,
            long batchSize)
            throws InterruptedException {
        try {
            MilvusSourceConverter sourceConverter = new MilvusSourceConverter(tableSchema);

            QueryIteratorParam.Builder param =
                    QueryIteratorParam.newBuilder()
                            .withDatabaseName(tablePath.getDatabaseName())
                            .withCollectionName(tablePath.getTableName())
                            .withOutFields(Lists.newArrayList("*"))
                            .withBatchSize(batchSize);

            if (StringUtils.isNotEmpty(partitionName)) {
                param.withPartitionNames(Collections.singletonList(partitionName));
            }

            R<QueryIterator> response = client.queryIterator(param.build());
            if (response.getStatus() != R.Status.Success.getCode()) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.SERVER_RESPONSE_FAILED, response.getException());
            }
            int maxFailRetry = 3;
            QueryIterator iterator = response.getData();
            while (maxFailRetry > 0) {
                try {
                    List<QueryResultsWrapper.RowRecord> next = iterator.next();
                    if (next == null || next.isEmpty()) {
                        break;
                    } else {
                        for (QueryResultsWrapper.RowRecord record : next) {
                            SeaTunnelRow seaTunnelRow =
                                    sourceConverter.convertToSeaTunnelRow(
                                            record, tableSchema, tablePath);
                            if (StringUtils.isNotEmpty(partitionName)) {
                                Map<String, Object> options = new HashMap<>();
                                options.put(CommonOptions.PARTITION.getName(), partitionName);
                                seaTunnelRow.setOptions(options);
                            }
                            output.collect(seaTunnelRow);
                        }
                    }
                } catch (Exception e) {
                    if (e.getMessage().contains("rate limit exceeded")) {
                        // for rateLimit, we can try iterator again after 30s, no need to update
                        // batch size directly
                        maxFailRetry--;
                        if (maxFailRetry == 0) {
                            log.error(
                                    "Iterate next data from milvus failed, batchSize = {}, throw exception",
                                    batchSize,
                                    e);
                            throw new MilvusConnectorException(
                                    MilvusConnectionErrorCode.READ_DATA_FAIL, e);
                        }
                        log.error(
                                "Iterate next data from milvus failed, batchSize = {}, will retry after 30 s, maxRetry: {}",
                                batchSize,
                                maxFailRetry,
                                e);
                        Thread.sleep(30000);
                    } else {
                        // if this error, we need to reduce batch size and try again, so throw
                        // exception here
                        throw new MilvusConnectorException(
                                MilvusConnectionErrorCode.READ_DATA_FAIL, e);
                    }
                }
            }
        } catch (Exception e) {
            if (e.getMessage().contains("rate limit exceeded") && batchSize > 10) {
                log.error(
                        "Query Iterate data from milvus failed, retry from beginning with smaller batch size: {} after 30 s",
                        batchSize / 2,
                        e);
                Thread.sleep(30000);
                queryIteratorData(tablePath, partitionName, tableSchema, output, batchSize / 2);
            } else {
                throw new MilvusConnectorException(MilvusConnectionErrorCode.READ_DATA_FAIL, e);
            }
        }
    }

    @Override
    public List<MilvusSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<MilvusSourceSplit> splits) {
        log.info("Adding milvus splits to reader: " + splits);
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this milvus reader will not add new split.");
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
