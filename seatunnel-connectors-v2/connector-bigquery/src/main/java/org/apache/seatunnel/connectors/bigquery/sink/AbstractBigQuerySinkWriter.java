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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.convert.BigQuerySerializer;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfo;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryWriter;

import org.json.JSONArray;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractBigQuerySinkWriter
        implements SinkWriter<SeaTunnelRow, BigQueryCommitInfo, BigQuerySinkState>,
                SupportMultiTableSinkWriter<Void> {
    protected final ReadonlyConfig config;
    protected final BigQuerySerializer serializer;
    protected final BigQueryWriteClient client;
    protected BigQueryWriter streamWriter;

    protected final int batchSize;
    protected JSONArray buffer = new JSONArray();

    protected AbstractBigQuerySinkWriter(
            ReadonlyConfig readOnlyConfig,
            BigQueryWriter streamWriter,
            BigQuerySerializer serializer,
            BigQueryWriteClient client) {
        this.config = readOnlyConfig;
        this.batchSize = readOnlyConfig.get(BigQuerySinkOptions.BATCH_SIZE);
        this.streamWriter = streamWriter;
        this.serializer = serializer;
        this.client = client;
    }

    protected void flush() {
        if (buffer.length() == 0) return;

        JSONArray dataToSend = buffer;
        buffer = new JSONArray();

        try {
            ApiFuture<AppendRowsResponse> future = streamWriter.append(dataToSend);
            future.get(60, TimeUnit.SECONDS);
            streamWriter.onAppendSuccess(dataToSend.length());
            log.info("Successfully appended {} rows.", dataToSend.length());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            buffer = dataToSend;
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.APPEND_ROWS_FAILED, e);
        } catch (Exception e) {
            buffer = dataToSend;
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.APPEND_ROWS_FAILED, e);
        }
    }

    protected boolean flushOnClose() {
        return true;
    }

    @Override
    public void close() {
        try {
            if (flushOnClose()) {
                flush();
            }
        } finally {
            try {
                streamWriter.close();
            } catch (Exception e) {
                log.warn("Failed to close streamWriter", e);
            }
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Failed to close BigQueryWriteClient", e);
            }
        }
    }
}
