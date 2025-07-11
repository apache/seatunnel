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
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class MaxcomputeOutputFormat {
    private final ReadonlyConfig readonlyConfig;

    private final TableSchema tableSchema;
    private final SeaTunnelRowType rowType;
    private final FormatterContext formatterContext;

    private UpsertStream upsertStream;
    private TableTunnel.UpsertSession upsertSession;

    public MaxcomputeOutputFormat(SeaTunnelRowType rowType, ReadonlyConfig readonlyConfig) {
        this.rowType = rowType;
        this.readonlyConfig = readonlyConfig;
        this.tableSchema = MaxcomputeUtil.getTable(readonlyConfig).getSchema();
        this.formatterContext =
                new FormatterContext(readonlyConfig.get(FormatOptions.DATETIME_FORMAT));
    }

    public void write(SeaTunnelRow seaTunnelRow) throws IOException, TunnelException {
        ensureUpsertSessionAndWriter();
        Record newRecord =
                MaxcomputeTypeMapper.getMaxcomputeRowData(
                        upsertSession.newRecord(),
                        seaTunnelRow,
                        this.tableSchema,
                        this.rowType,
                        formatterContext);

        switch (seaTunnelRow.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                upsertStream.upsert(newRecord);
                break;
            case DELETE:
                upsertStream.delete(newRecord);
                break;
            default:
                throw CommonError.unsupportedDataType(
                        MaxcomputeBaseOptions.PLUGIN_NAME,
                        seaTunnelRow.getRowKind().toString(),
                        seaTunnelRow.toString());
        }
    }

    public void close() throws IOException, TunnelException {
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
