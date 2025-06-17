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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.table.FormatOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Striped;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

@Slf4j
public class MaxcomputeOutputFormat {
    private static final int MIN_LOCK_COUNT = 16;
    private static final int MAX_LOCK_COUNT = 2048;
    private final Striped<Lock> stripedLocks;

    private final ReadonlyConfig readonlyConfig;

    private final TableSchema tableSchema;
    private final SeaTunnelRowType rowType;
    private final FormatterContext formatterContext;

    private RecordWriter recordWriter;
    private UpsertStream upsertStream;
    private TableTunnel.UploadSession uploadSession;
    private TableTunnel.UpsertSession upsertSession;

    public MaxcomputeOutputFormat(ReadonlyConfig readonlyConfig, SeaTunnelRowType rowType) {
        this.readonlyConfig = readonlyConfig;

        this.rowType = rowType;
        Table table = MaxcomputeUtil.getTable(readonlyConfig);
        this.tableSchema = table.getSchema();
        this.formatterContext =
                new FormatterContext(readonlyConfig.get(FormatOptions.DATETIME_FORMAT));

        int stripes =
                validateLockCount(readonlyConfig.get(MaxcomputeSinkOptions.UPSERT_LOCK_COUNT));
        this.stripedLocks = Striped.lock(stripes);
    }

    public void write(SeaTunnelRow seaTunnelRow) throws IOException, TunnelException {
        switch (seaTunnelRow.getRowKind()) {
            case INSERT:
                insertRecord(seaTunnelRow);
                break;
            case UPDATE_AFTER:
                upsertRecord(seaTunnelRow);
                break;
            case DELETE:
                deleteRecord(seaTunnelRow);
                break;
            default:
                throw new MaxcomputeConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported write row kind: " + seaTunnelRow.getRowKind());
        }
    }

    public void close() throws TunnelException, IOException {
        if (recordWriter != null) {
            recordWriter.close();
            uploadSession.commit();
            recordWriter = null;
        } else if (upsertStream != null) {
            upsertStream.close();
            upsertSession.commit(true);
            upsertStream = null;
        }
    }

    private int validateLockCount(int inputCount) {
        if (inputCount < MIN_LOCK_COUNT) {
            return MIN_LOCK_COUNT;
        }
        if (inputCount > MAX_LOCK_COUNT) {
            return MAX_LOCK_COUNT;
        }
        return inputCount;
    }

    private void insertRecord(SeaTunnelRow seaTunnelRow) throws TunnelException, IOException {
        ensureInsertSessionAndWriter();
        Record arrayRecord =
                MaxcomputeTypeMapper.getMaxcomputeRowData(
                        new ArrayRecord(tableSchema),
                        seaTunnelRow,
                        this.tableSchema,
                        this.rowType,
                        formatterContext);
        recordWriter.write(arrayRecord);
    }

    private void upsertRecord(SeaTunnelRow seaTunnelRow) throws TunnelException, IOException {
        ensureUpsertSessionAndWriter();
        Record upsertRecord =
                MaxcomputeTypeMapper.getMaxcomputeRowData(
                        upsertSession.newRecord(),
                        seaTunnelRow,
                        this.tableSchema,
                        this.rowType,
                        formatterContext);
        for (int i = 0; i < seaTunnelRow.getFields().length; i++) {
            String fieldName = rowType.getFieldName(i);
            upsertRecord.get(tableSchema.getColumnIndex(fieldName));
        }

        String pkKey = buildPrimaryKey(seaTunnelRow);

        Lock lock = stripedLocks.get(pkKey);
        lock.lock();
        try {
            upsertStream.upsert(upsertRecord);
        } finally {
            lock.unlock();
        }
    }

    private String buildPrimaryKey(SeaTunnelRow seaTunnelRow) throws JsonProcessingException {
        List<String> hashKeys = extractHashKeys();

        ObjectMapper mapper = new ObjectMapper();
        List<Object> pkValues = new ArrayList<>();

        for (int i = 0; i < seaTunnelRow.getFields().length; i++) {
            String fieldName = rowType.getFieldName(i);
            if (hashKeys.contains(fieldName)) {
                Object value = seaTunnelRow.getField(i);
                if (value == null)
                    throw new IllegalArgumentException(
                            "Primary key column '" + fieldName + "' must not be null.");
                pkValues.add(value);
            }
        }
        return mapper.writeValueAsString(pkValues);
    }

    private List<String> extractHashKeys() {
        List<String> hashKeys;
        try {
            Field field = upsertSession.getClass().getDeclaredField("hashKeys");
            field.setAccessible(true);
            hashKeys = (List<String>) field.get(upsertSession);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new MaxcomputeConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Failed to extract hashKeys via reflection",
                    e);
        }
        return hashKeys;
    }

    private void deleteRecord(SeaTunnelRow seaTunnelRow) throws TunnelException, IOException {
        ensureUpsertSessionAndWriter();
        Record deleteRecord =
                MaxcomputeTypeMapper.getMaxcomputeRowData(
                        upsertSession.newRecord(),
                        seaTunnelRow,
                        this.tableSchema,
                        this.rowType,
                        formatterContext);
        upsertStream.delete(deleteRecord);
    }

    private void ensureInsertSessionAndWriter() throws TunnelException {
        if (uploadSession == null) {
            initializeInsertSession();
        }
        if (uploadSession == null) {
            throw new IllegalStateException("UploadSession was not initialized properly");
        }

        if (recordWriter == null) {
            this.recordWriter = uploadSession.openBufferedWriter();
            log.info("open record writer success");
        }
        if (recordWriter == null) {
            throw new IllegalStateException("RecordWriter was not initialized properly");
        }
    }

    private void ensureUpsertSessionAndWriter() throws TunnelException, IOException {
        if (upsertSession == null) {
            initializeUpsertSession();
        }
        if (upsertSession == null) {
            throw new IllegalStateException("UploadSession was not initialized properly");
        }

        if (upsertStream == null) {
            this.upsertStream = upsertSession.buildUpsertStream().build();
            log.info("build upsert stream success");
        }
        if (upsertStream == null) {
            throw new IllegalStateException("RecordWriter was not initialized properly");
        }
    }

    private void initializeInsertSession() throws TunnelException {
        TableTunnel tunnel = MaxcomputeUtil.getTableTunnel(readonlyConfig);
        if (readonlyConfig.getOptional(MaxcomputeSinkOptions.PARTITION_SPEC).isPresent()) {
            PartitionSpec partitionSpec =
                    new PartitionSpec(readonlyConfig.get(MaxcomputeSinkOptions.PARTITION_SPEC));
            uploadSession =
                    tunnel.createUploadSession(
                            readonlyConfig.get(MaxcomputeSinkOptions.PROJECT),
                            readonlyConfig.get(MaxcomputeSinkOptions.TABLE_NAME),
                            partitionSpec);

        } else {
            uploadSession =
                    tunnel.createUploadSession(
                            readonlyConfig.get(MaxcomputeSinkOptions.PROJECT),
                            readonlyConfig.get(MaxcomputeSinkOptions.TABLE_NAME));
        }
    }

    private void initializeUpsertSession() throws TunnelException, IOException {
        TableTunnel tunnel = MaxcomputeUtil.getTableTunnel(readonlyConfig);
        if (readonlyConfig.getOptional(MaxcomputeSinkOptions.PARTITION_SPEC).isPresent()) {
            PartitionSpec partitionSpec =
                    new PartitionSpec(readonlyConfig.get(MaxcomputeSinkOptions.PARTITION_SPEC));
            upsertSession =
                    tunnel.buildUpsertSession(
                                    readonlyConfig.get(MaxcomputeSinkOptions.PROJECT),
                                    readonlyConfig.get(MaxcomputeSinkOptions.TABLE_NAME))
                            .setPartitionSpec(partitionSpec)
                            .build();

        } else {
            upsertSession =
                    tunnel.buildUpsertSession(
                                    readonlyConfig.get(MaxcomputeSinkOptions.PROJECT),
                                    readonlyConfig.get(MaxcomputeSinkOptions.TABLE_NAME))
                            .build();
        }
    }
}
