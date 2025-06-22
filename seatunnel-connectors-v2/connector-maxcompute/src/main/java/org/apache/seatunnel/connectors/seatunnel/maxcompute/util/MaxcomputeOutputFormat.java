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

import org.apache.seatunnel.shade.com.google.common.util.concurrent.Striped;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSinkOptions;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;

@Slf4j
public class MaxcomputeOutputFormat {
    private static final int MIN_LOCK_COUNT = 16;
    private static final int MAX_LOCK_COUNT = 2048;
    private final Striped<Lock> stripedLocks;
    private final PrimaryKey primaryKey;

    private final ReadonlyConfig readonlyConfig;

    private final TableSchema tableSchema;
    private final SeaTunnelRowType rowType;
    private final FormatterContext formatterContext;
    private final String tunnelEndPoint;

    private RecordWriter recordWriter;
    private UpsertStream upsertStream;
    private TableTunnel.UploadSession uploadSession;
    private TableTunnel.UpsertSession upsertSession;

    public MaxcomputeOutputFormat(
            SeaTunnelRowType rowType,
            ReadonlyConfig readonlyConfig,
            TableSchema tableSchema,
            FormatterContext formatterContext,
            String tunnelEndPoint,
            PrimaryKey primaryKey,
            int lockCount) {
        this.rowType = rowType;
        this.readonlyConfig = readonlyConfig;
        this.tableSchema = tableSchema;
        this.formatterContext = formatterContext;
        this.tunnelEndPoint = tunnelEndPoint;
        this.primaryKey = primaryKey;
        int stripes = validateLockCount(lockCount);
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
                throw CommonError.unsupportedDataType(
                        MaxcomputeBaseOptions.PLUGIN_NAME,
                        seaTunnelRow.getRowKind().toString(),
                        seaTunnelRow.toString());
        }
    }

    public void close() throws IOException, TunnelException {
        closeUploadSession();
        closeUpsertSession();
    }

    private void closeUploadSession() throws IOException, TunnelException {
        if (recordWriter != null) {
            try {
                recordWriter.close();
            } finally {
                recordWriter = null;
            }
        }
        if (uploadSession != null) {
            uploadSession.commit();
        }
    }

    private void closeUpsertSession() throws IOException, TunnelException {
        if (upsertStream != null) {
            try {
                upsertStream.flush();
                upsertStream.close();
            } finally {
                upsertStream = null;
            }
        }

        if (upsertSession != null) {
            try {
                upsertSession.commit(true);
            } finally {
                upsertSession.close();
                upsertSession = null;
            }
        }
    }

    int validateLockCount(int inputCount) {
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

        lockProcess(seaTunnelRow, () -> upsertStream.upsert(upsertRecord));
    }

    void lockProcess(SeaTunnelRow row, CheckedRunnable runnable)
            throws IOException, TunnelException {
        Lock lock = getLockByPrimaryKey(row);
        lock.lock();
        try {
            runnable.run();
        } catch (IOException | TunnelException e1) {
            throw e1;
        } catch (Exception e) {
            throw CommonError.illegalArgument(row.toString(), "Maxcompute upsert lockProcess");
        } finally {
            lock.unlock();
        }
    }

    Lock getLockByPrimaryKey(SeaTunnelRow seaTunnelRow) {
        int pkKey = buildPrimaryKey(seaTunnelRow);
        return stripedLocks.get(pkKey);
    }

    int buildPrimaryKey(SeaTunnelRow seaTunnelRow) {
        List<Object> pkValues = new ArrayList<>();
        for (int i = 0; i < seaTunnelRow.getFields().length; i++) {
            String fieldName = rowType.getFieldName(i);
            if (PrimaryKey.isPrimaryKeyField(primaryKey, fieldName)) {
                Object value = seaTunnelRow.getField(i);
                if (value == null)
                    throw CommonError.illegalArgument(
                            fieldName, "Primary key column must not be null.");
                pkValues.add(value);
            }
        }
        return Objects.hash(pkValues.toArray());
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
            Objects.requireNonNull(uploadSession, "UploadSession was not initialized properly");
        }
        if (recordWriter == null) {
            this.recordWriter = uploadSession.openBufferedWriter();
            log.info("open record writer success");
        }
    }

    private void ensureUpsertSessionAndWriter() throws TunnelException, IOException {
        if (upsertSession == null) {
            initializeUpsertSession();
            Objects.requireNonNull(upsertSession, "UpsertSession was not initialized properly");
        }
        if (upsertStream == null) {
            this.upsertStream = upsertSession.buildUpsertStream().build();
            log.info("build upsert stream success");
        }
    }

    private void initializeInsertSession() throws TunnelException {
        TableTunnel tunnel = getTableTunnel();
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

    private TableTunnel getTableTunnel() {
        TableTunnel tunnel = MaxcomputeUtil.getTableTunnel(readonlyConfig);
        if (this.tunnelEndPoint != null && !this.tunnelEndPoint.trim().isEmpty()) {
            tunnel.setEndpoint(this.tunnelEndPoint);
        }
        return tunnel;
    }

    private void initializeUpsertSession() throws TunnelException, IOException {
        TableTunnel tunnel = getTableTunnel();
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

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }
}
