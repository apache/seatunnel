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
import com.google.protobuf.Descriptors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.TableSchemaUtil.createStreamWriter;
import static org.apache.seatunnel.connectors.bigquery.sink.writer.TableSchemaUtil.getActualTableSchema;

@Slf4j
public class BigQueryStreamWriter implements BigQueryWriter {
    public static final String DEFAULT_PATH = "/streams/_default";
    public static final String CHANGE_TYPE = "_CHANGE_TYPE";
    public static final String SEQUENCE_NUM = "_CHANGE_SEQUENCE_NUMBER";
    private final JsonStreamWriter streamWriter;
    @Getter private final String streamName;
    @Getter private final String tablePath;

    public BigQueryStreamWriter(
            JsonStreamWriter streamWriter, String streamName, String tablePath) {
        this.streamWriter = streamWriter;
        this.streamName = streamName;
        this.tablePath = tablePath;
    }

    public static BigQueryStreamWriter of(BigQueryWriteClient client, ReadonlyConfig config) {
        String projectId = config.get(BigQuerySinkOptions.PROJECT_ID);
        String datasetId = config.get(BigQuerySinkOptions.DATASET_ID);
        String tableId = config.get(BigQuerySinkOptions.TABLE_ID);
        String parentTable = TableName.of(projectId, datasetId, tableId).toString();
        TableSchema tableSchema = getActualTableSchema(config, true);

        String streamName = createStreamName(projectId, datasetId, tableId);
        log.info("Created Default write stream {}", streamName);
        return new BigQueryStreamWriter(
                createStreamWriter(streamName, tableSchema, client), streamName, parentTable);
    }

    private static String createStreamName(String projectId, String datasetId, String tableId) {
        return TableName.of(projectId, datasetId, tableId).toString() + DEFAULT_PATH;
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
}
