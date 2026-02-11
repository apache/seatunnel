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

    public BigQueryBatchWriter(
            JsonStreamWriter streamWriter,
            BigQueryWriteClient client,
            String streamName,
            String tablePath) {
        this.streamWriter = streamWriter;
        this.client = client;
        this.streamName = streamName;
        this.tablePath = tablePath;
    }

    public static BigQueryBatchWriter of(BigQueryWriteClient client, ReadonlyConfig config) {
        return BigQueryBatchWriter.of(client, config, getActualTableSchema(config, false));
    }

    public static BigQueryBatchWriter of(
            BigQueryWriteClient client, ReadonlyConfig config, TableSchema tableSchema) {
        String projectId = config.get(BigQuerySinkOptions.PROJECT_ID);
        String datasetId = config.get(BigQuerySinkOptions.DATASET_ID);
        String tableId = config.get(BigQuerySinkOptions.TABLE_ID);
        String parentTable = TableName.of(projectId, datasetId, tableId).toString();

        WriteStream writeStream =
                WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();
        WriteStream createdStream = client.createWriteStream(parentTable, writeStream);

        String assignedStreamName = createdStream.getName();
        log.info("Created Pending write stream {}", assignedStreamName);
        return new BigQueryBatchWriter(
                createStreamWriter(assignedStreamName, tableSchema, client),
                client,
                assignedStreamName,
                parentTable);
    }

    @Override
    public ApiFuture<AppendRowsResponse> append(JSONArray jsonArr)
            throws Descriptors.DescriptorValidationException, IOException {
        return streamWriter.append(jsonArr);
    }

    @Override
    public void close() {
        streamWriter.close();
    }

    @Override
    public void finalizeStream() {
        client.finalizeWriteStream(streamName);
    }
}
