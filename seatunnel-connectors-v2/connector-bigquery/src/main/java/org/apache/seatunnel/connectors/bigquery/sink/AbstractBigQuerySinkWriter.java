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
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.Exceptions.SchemaMismatchedException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractBigQuerySinkWriter
        implements SinkWriter<SeaTunnelRow, BigQueryCommitInfo, BigQuerySinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {
    private static final long SCHEMA_PROPAGATION_RETRY_TIMEOUT_MILLIS =
            TimeUnit.MINUTES.toMillis(5);
    private static final long SCHEMA_PROPAGATION_INITIAL_RETRY_DELAY_MILLIS = 1_000L;
    private static final long SCHEMA_PROPAGATION_MAX_RETRY_DELAY_MILLIS = 10_000L;

    protected final ReadonlyConfig config;
    protected BigQuerySerializer serializer;
    protected final BigQueryWriteClient client;
    protected BigQueryWriter streamWriter;
    protected TableSchema tableSchema;

    protected final int batchSize;
    protected JSONArray buffer = new JSONArray();
    private BigQuerySchemaChangeManager schemaChangeManager;
    private boolean storageSchemaPropagationPending;

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
            appendRows(dataToSend);
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

    /**
     * Retries the first append after a schema change while the Storage Write API catches up with
     * the table metadata. BigQuery can expose the new column through the REST API before the
     * Storage Write API accepts rows using it.
     */
    protected AppendRowsResponse appendRows(JSONArray dataToSend) throws Exception {
        if (!storageSchemaPropagationPending) {
            ApiFuture<AppendRowsResponse> future = streamWriter.append(dataToSend);
            return future.get(60, TimeUnit.SECONDS);
        }

        long retryDeadline = 0L;
        int retryCount = 0;

        while (true) {
            try {
                ApiFuture<AppendRowsResponse> future = streamWriter.append(dataToSend);
                AppendRowsResponse response = future.get(60, TimeUnit.SECONDS);
                if (storageSchemaPropagationPending && isSchemaMismatch(response)) {
                    if (retryDeadline == 0L) {
                        retryDeadline =
                                System.currentTimeMillis()
                                        + SCHEMA_PROPAGATION_RETRY_TIMEOUT_MILLIS;
                    }
                    if (!retrySchemaPropagation(retryDeadline, retryCount++, dataToSend.length())) {
                        throw new BigQueryConnectorException(
                                BigQueryConnectorErrorCode.APPEND_ROWS_FAILED,
                                "BigQuery Storage Write API did not detect the updated table "
                                        + "schema within 5 minutes: "
                                        + response.getError().getMessage());
                    }
                    continue;
                }
                if (!response.hasError()) {
                    storageSchemaPropagationPending = false;
                }
                return response;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (!storageSchemaPropagationPending || !isSchemaMismatch(e)) {
                    throw e;
                }
                if (retryDeadline == 0L) {
                    retryDeadline =
                            System.currentTimeMillis() + SCHEMA_PROPAGATION_RETRY_TIMEOUT_MILLIS;
                }
                if (!retrySchemaPropagation(retryDeadline, retryCount++, dataToSend.length())) {
                    throw e;
                }
            }
        }
    }

    private boolean retrySchemaPropagation(long retryDeadline, int retryCount, int rowCount)
            throws InterruptedException {
        long remainingMillis = retryDeadline - System.currentTimeMillis();
        if (remainingMillis <= 0) {
            return false;
        }

        long retryDelayMillis =
                Math.min(
                        SCHEMA_PROPAGATION_INITIAL_RETRY_DELAY_MILLIS << Math.min(retryCount, 4),
                        SCHEMA_PROPAGATION_MAX_RETRY_DELAY_MILLIS);
        retryDelayMillis = Math.min(retryDelayMillis, remainingMillis);
        log.warn(
                "BigQuery Storage Write API has not detected the updated table schema yet. "
                        + "Retrying the same {} rows in {} ms.",
                rowCount,
                retryDelayMillis);
        waitForSchemaPropagation(retryDelayMillis);
        if (streamWriter.isClosed()) {
            log.warn(
                    "The BigQuery writer closed while waiting for schema propagation. "
                            + "Recreating it before retrying.");
            streamWriter = streamWriter.refreshSchema(client, config);
        }
        return true;
    }

    void waitForSchemaPropagation(long delayMillis) throws InterruptedException {
        Thread.sleep(delayMillis);
    }

    private boolean isSchemaMismatch(AppendRowsResponse response) {
        return response != null
                && response.hasError()
                && isSchemaMismatch(Exceptions.toStorageException(response.getError(), null));
    }

    private boolean isSchemaMismatch(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SchemaMismatchedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
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
        this.storageSchemaPropagationPending = true;
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
