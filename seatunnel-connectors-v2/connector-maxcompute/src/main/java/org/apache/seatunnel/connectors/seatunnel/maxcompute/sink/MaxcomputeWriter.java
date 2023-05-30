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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.TABLE_NAME;

@Slf4j
public class MaxcomputeWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private RecordWriter recordWriter;
    private final TableTunnel.UploadSession session;
    private final TableSchema tableSchema;
    private static final Long BLOCK_0 = 0L;

    public MaxcomputeWriter(Config pluginConfig) {
        try {
            Table table = MaxcomputeUtil.getTable(pluginConfig);
            this.tableSchema = table.getSchema();
            TableTunnel tunnel = MaxcomputeUtil.getTableTunnel(pluginConfig);
            if (pluginConfig.hasPath(PARTITION_SPEC.key())) {
                PartitionSpec partitionSpec =
                        new PartitionSpec(pluginConfig.getString(PARTITION_SPEC.key()));
                session =
                        tunnel.createUploadSession(
                                pluginConfig.getString(PROJECT.key()),
                                pluginConfig.getString(TABLE_NAME.key()),
                                partitionSpec);
            } else {
                session =
                        tunnel.createUploadSession(
                                pluginConfig.getString(PROJECT.key()),
                                pluginConfig.getString(TABLE_NAME.key()));
            }
            this.recordWriter = session.openRecordWriter(BLOCK_0);
            log.info("open record writer success");
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(CommonErrorCode.WRITER_OPERATION_FAILED, e);
        }
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws IOException {
        Record record = MaxcomputeTypeMapper.getMaxcomputeRowData(seaTunnelRow, this.tableSchema);
        recordWriter.write(record);
    }

    @Override
    public void close() throws IOException {
        if (recordWriter != null) {
            recordWriter.close();
            try {
                session.commit(new Long[] {BLOCK_0});
            } catch (Exception e) {
                throw new MaxcomputeConnectorException(CommonErrorCode.WRITER_OPERATION_FAILED, e);
            }
            recordWriter = null;
        }
    }
}
