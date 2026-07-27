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
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.convert.BigQuerySerializer;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.schema.BigQuerySchemaChangeManager;
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
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {
    protected final ReadonlyConfig config;
    protected BigQuerySerializer serializer;
    protected final BigQueryWriteClient client;
    protected BigQueryWriter streamWriter;
    protected TableSchema tableSchema;

    protected final int batchSize;
    protected JSONArray buffer = new JSONArray();
    private BigQuerySchemaChangeManager schemaChangeManager;

    protected AbstractBigQuerySinkWriter(
            ReadonlyConfig readOnlyConfig,
            BigQueryWriter streamWriter,
            BigQuerySerializer serializer,
            BigQueryWriteClient client) {
        this(readOnlyConfig, streamWriter, serializer, null, client);
    }

    protected AbstractBigQuerySinkWriter(
            ReadonlyConfig readOnlyConfig,
            BigQueryWriter streamWriter,
            BigQuerySerializer serializer,
            TableSchema tableSchema,
            BigQueryWriteClient client) {
        this.config = readOnlyConfig;
        this.batchSize = readOnlyConfig.get(BigQuerySinkOptions.BATCH_SIZE);
        this.streamWriter = streamWriter;
        this.serializer = serializer;
        this.tableSchema = tableSchema;
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

    /**
     * Flushes rows encoded with the old schema, applies the BigQuery DDL, and rebuilds both the row
     * serializer and JSON stream writer before rows with the new schema are accepted.
     */
    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        if (!config.get(BigQuerySinkOptions.SCHEMA_EVOLUTION_ENABLED)) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    "Received schema change event but schema_evolution_enabled=false. "
                            + "Enable it on the BigQuery sink or disable schema-changes.enabled "
                            + "on the CDC source.");
        }

        if (tableSchema == null) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    "The current SeaTunnel table schema is required to apply a BigQuery schema "
                            + "change.");
        }
        TableSchema evolvedTableSchema =
                new AlterTableSchemaEventHandler().reset(tableSchema).apply(event);
        BigQuerySerializer evolvedSerializer =
                new BigQuerySerializer(evolvedTableSchema.toPhysicalRowDataType(), config);

        flush();
        getSchemaChangeManager().applySchemaChange(event);
        BigQueryWriter evolvedWriter = streamWriter.refreshSchema(client, config);

        this.tableSchema = evolvedTableSchema;
        this.serializer = evolvedSerializer;
        this.streamWriter = evolvedWriter;
    }

    private BigQuerySchemaChangeManager getSchemaChangeManager() {
        if (schemaChangeManager == null) {
            schemaChangeManager = new BigQuerySchemaChangeManager(config);
        }
        return schemaChangeManager;
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
