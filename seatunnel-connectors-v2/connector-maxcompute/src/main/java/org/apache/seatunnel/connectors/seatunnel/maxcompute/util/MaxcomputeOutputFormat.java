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

@Slf4j
public class MaxcomputeOutputFormat {
    private static final String UPLOAD_SESSION = "upload";
    private static final String UPSERT_SESSION = "upsert";

    private final SeaTunnelRowType rowType;
    private final ReadonlyConfig readonlyConfig;
    private final TableSchema tableSchema;
    private final FormatterContext formatterContext;
    private final boolean isUploadSession;

    private RecordWriter recordWriter;
    private UpsertStream upsertStream;
    private TableTunnel.UploadSession uploadSession;
    private TableTunnel.UpsertSession upsertSession;

    public MaxcomputeOutputFormat(SeaTunnelRowType rowType, ReadonlyConfig readonlyConfig) {
        this.rowType = rowType;
        this.readonlyConfig = readonlyConfig;
        this.tableSchema = MaxcomputeUtil.getTable(readonlyConfig).getSchema();
        this.formatterContext =
                new FormatterContext(readonlyConfig.get(FormatOptions.DATETIME_FORMAT));

        String insertStrategy = readonlyConfig.get(MaxcomputeSinkOptions.INSERT_STRATEGY);
        if (UPLOAD_SESSION.equals(insertStrategy)) {
            isUploadSession = true;
        } else if (UPSERT_SESSION.equals(insertStrategy)) {
            isUploadSession = false;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot resolve insert strategy: [%s]. Supported values are: '%s', '%s'",
                            insertStrategy, UPLOAD_SESSION, UPSERT_SESSION));
        }
    }

    public void write(SeaTunnelRow seaTunnelRow) throws IOException, TunnelException {
        switch (seaTunnelRow.getRowKind()) {
            case INSERT:
                if (isUploadSession) {
                    insertRecord(seaTunnelRow);
                } else {
                    upsertRecord(seaTunnelRow);
                }
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
        Record upsertRecord = getNewRecord(seaTunnelRow);
        upsertStream.upsert(upsertRecord);
    }

    private void deleteRecord(SeaTunnelRow seaTunnelRow) throws TunnelException, IOException {
        Record deleteRecord = getNewRecord(seaTunnelRow);
        upsertStream.delete(deleteRecord);
    }

    private Record getNewRecord(SeaTunnelRow seaTunnelRow) throws TunnelException, IOException {
        ensureUpsertSessionAndWriter();
        return MaxcomputeTypeMapper.getMaxcomputeRowData(
                upsertSession.newRecord(),
                seaTunnelRow,
                this.tableSchema,
                this.rowType,
                formatterContext);
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

    private void ensureUpsertSessionAndWriter() throws TunnelException, IOException {
        if (upsertSession == null) {
            initializeUpsertSession();
        }
        if (upsertStream == null) {
            this.upsertStream = upsertSession.buildUpsertStream().build();
            log.info("build upsert stream success");
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
                            .setSchemaName(tunnel.getConfig().getOdps().getCurrentSchema())
                            .build();
        } else {
            upsertSession =
                    tunnel.buildUpsertSession(
                                    readonlyConfig.get(MaxcomputeSinkOptions.PROJECT),
                                    readonlyConfig.get(MaxcomputeSinkOptions.TABLE_NAME))
                            .setSchemaName(tunnel.getConfig().getOdps().getCurrentSchema())
                            .build();
        }
    }

    private void ensureInsertSessionAndWriter() throws TunnelException {
        if (uploadSession == null) {
            initializeInsertSession();
        }
        if (recordWriter == null) {
            this.recordWriter = uploadSession.openBufferedWriter();
            log.info("open record writer success");
        }
    }

    private void initializeInsertSession() throws TunnelException {
        TableTunnel tunnel = MaxcomputeUtil.getTableTunnel(readonlyConfig);
        PartitionSpec partitionSpec = null;
        if (readonlyConfig.getOptional(MaxcomputeSinkOptions.PARTITION_SPEC).isPresent()) {
            partitionSpec =
                    new PartitionSpec(readonlyConfig.get(MaxcomputeSinkOptions.PARTITION_SPEC));
        }
        uploadSession =
                MaxcomputeUtil.buildUploadSession(
                        tunnel,
                        readonlyConfig.get(MaxcomputeSinkOptions.PROJECT),
                        readonlyConfig.get(MaxcomputeSinkOptions.TABLE_NAME),
                        partitionSpec);
    }
}
