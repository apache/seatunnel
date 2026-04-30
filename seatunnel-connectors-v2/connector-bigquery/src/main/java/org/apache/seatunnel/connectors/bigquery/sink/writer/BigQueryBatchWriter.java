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

package org.apache.seatunnel.connectors.bigquery.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import org.json.JSONArray;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.Descriptors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.TableSchemaUtil.createStreamWriter;
import static org.apache.seatunnel.connectors.bigquery.sink.writer.TableSchemaUtil.getActualTableSchema;

@Slf4j
public class BigQueryBatchWriter implements BigQueryWriter {
    private final JsonStreamWriter streamWriter;
    private final BigQueryWriteClient client;

    @Getter private final String streamName;
    @Getter private final String tablePath;
    @Getter private long nextOffset;

    public BigQueryBatchWriter(
            JsonStreamWriter streamWriter,
            BigQueryWriteClient client,
            String streamName,
            String tablePath,
            long nextOffset) {
        this.streamWriter = streamWriter;
        this.client = client;
        this.streamName = streamName;
        this.tablePath = tablePath;
        this.nextOffset = nextOffset;
    }

    public static BigQueryBatchWriter of(BigQueryWriteClient client, ReadonlyConfig config) {
        return BigQueryBatchWriter.of(client, config, getActualTableSchema(config, false));
    }

    public static BigQueryBatchWriter of(
            BigQueryWriteClient client, ReadonlyConfig config, TableSchema tableSchema) {
        return BigQueryBatchWriter.of(client, config, tableSchema, null, 0L);
    }

    public static BigQueryBatchWriter restore(
            BigQueryWriteClient client, ReadonlyConfig config, String streamName, long nextOffset) {
        return BigQueryBatchWriter.of(
                client, config, getActualTableSchema(config, false), streamName, nextOffset);
    }

    private static BigQueryBatchWriter of(
            BigQueryWriteClient client,
            ReadonlyConfig config,
            TableSchema tableSchema,
            String restoredStreamName,
            long nextOffset) {
        String projectId = config.get(BigQuerySinkOptions.PROJECT_ID);
        String datasetId = config.get(BigQuerySinkOptions.DATASET_ID);
        String tableId = config.get(BigQuerySinkOptions.TABLE_ID);
        String parentTable = TableName.of(projectId, datasetId, tableId).toString();

        String assignedStreamName = restoredStreamName;
        if (assignedStreamName == null || assignedStreamName.isEmpty()) {
            WriteStream writeStream =
                    WriteStream.newBuilder().setType(WriteStream.Type.BUFFERED).build();
            WriteStream createdStream = client.createWriteStream(parentTable, writeStream);
            assignedStreamName = createdStream.getName();
            log.info("Created Buffered write stream {}", assignedStreamName);
        } else {
            log.info(
                    "Restored Buffered write stream {} at nextOffset {}",
                    assignedStreamName,
                    nextOffset);
        }

        return new BigQueryBatchWriter(
                createStreamWriter(assignedStreamName, tableSchema, client),
                client,
                assignedStreamName,
                parentTable,
                nextOffset);
    }

    @Override
    public ApiFuture<AppendRowsResponse> append(JSONArray jsonArr)
            throws Descriptors.DescriptorValidationException, IOException {
        long appendOffset = nextOffset;
        return streamWriter.append(jsonArr, appendOffset);
    }

    @Override
    public void onAppendSuccess(int rowCount) {
        nextOffset += rowCount;
    }

    @Override
    public void close() {
        streamWriter.close();
    }
}
