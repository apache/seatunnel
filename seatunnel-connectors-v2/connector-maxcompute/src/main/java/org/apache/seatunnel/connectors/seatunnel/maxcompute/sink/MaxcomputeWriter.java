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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.table.FormatOptions;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.FormatterContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import com.aliyun.odps.TableSchema;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class MaxcomputeWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {
    private MaxcomputeOutputFormat writer;

    public MaxcomputeWriter(
            ReadonlyConfig readonlyConfig, SeaTunnelRowType rowType, PrimaryKey primaryKey) {
        try {
            TableSchema schema = MaxcomputeUtil.getTable(readonlyConfig).getSchema();
            FormatterContext formatterContext =
                    new FormatterContext(readonlyConfig.get(FormatOptions.DATETIME_FORMAT));
            int lockCount = readonlyConfig.get(MaxcomputeSinkOptions.UPSERT_LOCK_COUNT);
            String tunnelEndpoint = readonlyConfig.get(MaxcomputeSinkOptions.TUNNEL_ENDPOINT);
            writer =
                    new MaxcomputeOutputFormat(
                            rowType,
                            readonlyConfig,
                            schema,
                            formatterContext,
                            tunnelEndpoint,
                            primaryKey,
                            lockCount);
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, e);
        }
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws IOException {
        try {
            writer.write(seaTunnelRow);
        } catch (IOException e1) {
            throw e1;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e2) {
            throw CommonError.writeSeaTunnelRowFailed(
                    MaxcomputeBaseOptions.PLUGIN_NAME, seaTunnelRow.toString(), e2);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (IOException e1) {
            throw e1;
        } catch (Exception e2) {
            throw CommonError.closeFailed(MaxcomputeBaseOptions.PLUGIN_NAME, e2);
        }
    }
}
