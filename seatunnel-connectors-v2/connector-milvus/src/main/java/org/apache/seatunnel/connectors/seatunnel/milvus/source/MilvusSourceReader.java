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
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.GetLoadStateResponse;
import io.milvus.grpc.LoadState;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.GetLoadStateParam;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.response.QueryResultsWrapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

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
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            MilvusSourceSplit split = pendingSplits.poll();
            if (null != split) {
                handleEveryRowInternal(split, output);
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

    private void handleEveryRowInternal(MilvusSourceSplit split, Collector<SeaTunnelRow> output) {
        TablePath tablePath = split.getTablePath();
        TableSchema tableSchema = sourceTables.get(tablePath).getTableSchema();
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

        QueryIteratorParam param =
                QueryIteratorParam.newBuilder()
                        .withDatabaseName(tablePath.getDatabaseName())
                        .withCollectionName(tablePath.getTableName())
                        .withOutFields(Lists.newArrayList("*"))
                        .build();

        R<QueryIterator> response = client.queryIterator(param);
        if (response.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.SERVER_RESPONSE_FAILED,
                    loadStateResponse.getException());
        }

        QueryIterator iterator = response.getData();
        while (true) {
            List<QueryResultsWrapper.RowRecord> next = iterator.next();
            if (next == null || next.isEmpty()) {
                break;
            } else {
                for (QueryResultsWrapper.RowRecord record : next) {
                    SeaTunnelRow seaTunnelRow =
                            convertToSeaTunnelRow(record, tableSchema, tablePath);
                    output.collect(seaTunnelRow);
                }
            }
        }
    }

    public SeaTunnelRow convertToSeaTunnelRow(
            QueryResultsWrapper.RowRecord record, TableSchema tableSchema, TablePath tablePath) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[record.getFieldValues().size()];
        Map<String, Object> fieldValuesMap = record.getFieldValues();
        String[] fieldNames = typeInfo.getFieldNames();
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            Object filedValues = fieldValuesMap.get(fieldNames[fieldIndex]);
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = filedValues.toString();
                    break;
                case BOOLEAN:
                    if (filedValues instanceof Boolean) {
                        fields[fieldIndex] = filedValues;
                    } else {
                        fields[fieldIndex] = Boolean.valueOf(filedValues.toString());
                    }
                    break;
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
        log.info("Adding milvus splits to reader: {}", splits);
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
