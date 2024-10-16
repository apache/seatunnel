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
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig.RATE_LIMIT;

@Slf4j
public class MilvusSourceReader implements SourceReader<SeaTunnelRow, MilvusSourceSplit> {

    private final Deque<MilvusSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final ReadonlyConfig config;
    private final Context context;
    private Map<TablePath, CatalogTable> sourceTables;

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
                    handleEveryRowInternal(split, output);
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

    private void handleEveryRowInternal(MilvusSourceSplit split, Collector<SeaTunnelRow> output)
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
                                    convertToSeaTunnelRow(record, tableSchema, tablePath);
                            if (StringUtils.isNotEmpty(partitionName)) {
                                seaTunnelRow.setPartitionName(partitionName);
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

    public SeaTunnelRow convertToSeaTunnelRow(
            QueryResultsWrapper.RowRecord record, TableSchema tableSchema, TablePath tablePath) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        Map<String, Object> fieldValuesMap = record.getFieldValues();
        String[] fieldNames = typeInfo.getFieldNames();
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            Object filedValues = fieldValuesMap.get(fieldNames[fieldIndex]);
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = filedValues.toString();
                    break;
                case JSON:
                    fields[fieldIndex] = filedValues;
                    break;
                case BOOLEAN:
                    if (filedValues instanceof Boolean) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Boolean.valueOf(filedValues.toString());
                    }
                    break;
                case TINYINT:
                    if (filedValues instanceof Byte) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Byte.parseByte(filedValues.toString());
                    }
                    break;
                case SMALLINT:
                    if (filedValues instanceof Short) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Short.parseShort(filedValues.toString());
                    }
                case INT:
                    if (filedValues instanceof Integer) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Integer.valueOf(filedValues.toString());
                    }
                    break;
                case BIGINT:
                    if (filedValues instanceof Long) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Long.parseLong(filedValues.toString());
                    }
                    break;
                case FLOAT:
                    if (filedValues instanceof Float) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Float.parseFloat(filedValues.toString());
                    }
                    break;
                case DOUBLE:
                    if (filedValues instanceof Double) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Double.parseDouble(filedValues.toString());
                    }
                    break;
                case ARRAY:
                    if (filedValues instanceof List) {
                        List<?> list = (List<?>) filedValues;
                        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelDataType;
                        SqlType elementType = arrayType.getElementType().getSqlType();
                        switch (elementType) {
                            case STRING:
                                String[] arrays = new String[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    arrays[i] = list.get(i).toString();
                                }
                                fields[fieldIndex] = arrays;
                                break;
                            case BOOLEAN:
                                Boolean[] booleanArrays = new Boolean[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    booleanArrays[i] = Boolean.valueOf(list.get(i).toString());
                                }
                                fields[fieldIndex] = booleanArrays;
                                break;
                            case TINYINT:
                                Byte[] byteArrays = new Byte[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    byteArrays[i] = Byte.parseByte(list.get(i).toString());
                                }
                                fields[fieldIndex] = byteArrays;
                                break;
                            case SMALLINT:
                                Short[] shortArrays = new Short[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    shortArrays[i] = Short.parseShort(list.get(i).toString());
                                }
                                fields[fieldIndex] = shortArrays;
                                break;
                            case INT:
                                Integer[] intArrays = new Integer[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    intArrays[i] = Integer.valueOf(list.get(i).toString());
                                }
                                fields[fieldIndex] = intArrays;
                                break;
                            case BIGINT:
                                Long[] longArrays = new Long[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    longArrays[i] = Long.parseLong(list.get(i).toString());
                                }
                                fields[fieldIndex] = longArrays;
                                break;
                            case FLOAT:
                                Float[] floatArrays = new Float[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    floatArrays[i] = Float.parseFloat(list.get(i).toString());
                                }
                                fields[fieldIndex] = floatArrays;
                                break;
                            case DOUBLE:
                                Double[] doubleArrays = new Double[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    doubleArrays[i] = Double.parseDouble(list.get(i).toString());
                                }
                                fields[fieldIndex] = doubleArrays;
                                break;
                            default:
                                throw new MilvusConnectorException(
                                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                        "Unexpected array value: " + filedValues);
                        }
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected array value: " + filedValues);
                    }
                    break;
                case FLOAT_VECTOR:
                    if (filedValues instanceof List) {
                        List list = (List) filedValues;
                        Float[] arrays = new Float[list.size()];
                        for (int i = 0; i < list.size(); i++) {
                            arrays[i] = Float.parseFloat(list.get(i).toString());
                        }
                        fields[fieldIndex] = BufferUtils.toByteBuffer(arrays);
                        break;
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected vector value: " + filedValues);
                    }
                case BINARY_VECTOR:
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                    if (filedValues instanceof ByteBuffer) {
                        fields[fieldIndex] = filedValues;
                        break;
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected vector value: " + filedValues);
                    }
                case SPARSE_FLOAT_VECTOR:
                    if (filedValues instanceof Map) {
                        fields[fieldIndex] = filedValues;
                        break;
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected vector value: " + filedValues);
                    }
                default:
                    throw new MilvusConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType.getSqlType().name());
            }
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setTableId(tablePath.getFullName());
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
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
