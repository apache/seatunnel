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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.coordinator.SchemaCoordinator;
import org.apache.seatunnel.api.table.coordinator.SchemaResponse;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.FlushEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.event.TableEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaCoordinationException;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaValidationException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** operators added to the source and transformer pipelines to handle schema evolution */
@Slf4j
public class SchemaOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {
    private final Map<TableIdentifier, CatalogTable> localSchemaState;
    private String jobId;
    private final SupportSchemaEvolution source;
    private final Config pluginConfig;
    private SchemaCoordinator schemaCoordinator;
    private final AtomicReference<CompletableFuture<SchemaResponse>> currentSchemaChangeFuture =
            new AtomicReference<>();
    private volatile Long lastProcessedEventTime;

    public SchemaOperator(String jobId, SupportSchemaEvolution source, Config pluginConfig) {
        this.jobId = jobId;
        this.source = source;
        this.pluginConfig = pluginConfig;
        this.localSchemaState = new ConcurrentHashMap<>();
        this.schemaCoordinator = SchemaCoordinator.getOrCreateInstance(jobId);
    }

    @Override
    public void open() throws Exception {
        super.open();
        try {
            String flinkJobId = getRuntimeContext().getJobId().toString();
            if (!flinkJobId.equals(this.jobId)) {
                log.info(
                        "Updating SchemaCoordinator from SeaTunnel jobId {} to Flink jobId {}",
                        this.jobId,
                        flinkJobId);
                this.jobId = flinkJobId;
                this.schemaCoordinator = SchemaCoordinator.getOrCreateInstance(flinkJobId);
            }
        } catch (Exception e) {
            log.warn("Failed to get Flink jobId, using SeaTunnel jobId: {}", this.jobId, e);
        }
    }

    @Override
    public void processElement(StreamRecord<SeaTunnelRow> streamRecord) throws Exception {
        SeaTunnelRow element = streamRecord.getValue();

        if (!isSchemaEvolutionEnabled(pluginConfig)) {
            output.collect(new StreamRecord<>(element, streamRecord.getTimestamp()));
            return;
        }

        if ("__SCHEMA_CHANGE_EVENT__".equals(element.getTableId())
                && element.getOptions() != null) {
            Object object = element.getOptions().get("schema_change_event");
            if (object instanceof SchemaChangeEvent) {
                handleSchemaChangeEvent((SchemaChangeEvent) object);
                return;
            }
        }

        output.collect(new StreamRecord<>(element, streamRecord.getTimestamp()));
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) throws Exception {
        List<SchemaChangeType> supportedTypes = source.supports();
        if (supportedTypes == null || supportedTypes.isEmpty()) {
            log.info(
                    "Source: {} does not support any schema change types, skipping schema change event",
                    source);
            return;
        }

        if (!isSchemaChangeSupported(schemaChangeEvent, supportedTypes)) {
            log.warn(
                    "Schema change type {} not supported by source {}, skipping",
                    schemaChangeEvent.getEventType(),
                    source);
            return;
        }

        processSchemaChangeEvent(schemaChangeEvent);
    }

    private boolean isSchemaEvolutionEnabled(Config pluginConfig) {
        if (pluginConfig.hasPath("schema-changes.enabled")) {
            return pluginConfig.getBoolean("schema-changes.enabled");
        }

        if (pluginConfig.hasPath("debezium")) {
            Config debeziumConfig = pluginConfig.getConfig("debezium");
            if (debeziumConfig.hasPath("schema.changes.enabled")) {
                return debeziumConfig.getBoolean("schema.changes.enabled");
            }
        }

        return false;
    }

    private boolean isSchemaChangeSupported(
            SchemaChangeEvent event, List<SchemaChangeType> supportedTypes) {
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN);
            case SCHEMA_CHANGE_DROP_COLUMN:
                return supportedTypes.contains(SchemaChangeType.DROP_COLUMN);
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                return supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN);
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                return supportedTypes.contains(SchemaChangeType.RENAME_COLUMN);
            case SCHEMA_CHANGE_UPDATE_COLUMNS:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.DROP_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.RENAME_COLUMN);
            default:
                log.error("Unknown schema change event type: {}", event.getEventType());
                throw SchemaValidationException.unsupportedChangeType(
                        event.tableIdentifier(), jobId);
        }
    }

    private void processSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) throws Exception {
        TableIdentifier tableId = schemaChangeEvent.tableIdentifier();
        long eventTime = schemaChangeEvent.getCreatedTime();

        if (lastProcessedEventTime != null && eventTime <= lastProcessedEventTime) {
            throw SchemaValidationException.outdatedEvent(
                    tableId, jobId, eventTime, lastProcessedEventTime);
        }

        // set the jobId for the schema change event to ensure proper coordination
        if (schemaChangeEvent instanceof TableEvent) {
            schemaChangeEvent.setJobId(jobId);
        }

        log.info(
                "Handling schema change event for table: {}, job: {}, event time: {}",
                tableId,
                jobId,
                eventTime);

        try {
            CompletableFuture<SchemaResponse> schemaChangeFuture =
                    schemaCoordinator.requestSchemaChange(
                            tableId, jobId, schemaChangeEvent.getChangeAfter(), 1);
            currentSchemaChangeFuture.set(schemaChangeFuture);
            sendFlushEventToDownstream(schemaChangeEvent);

            SchemaResponse response;
            try {
                response = schemaChangeFuture.get(60, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                log.error(
                        "Schema change timeout for table: {}, directly failing the job",
                        tableId,
                        e);
                throw SchemaCoordinationException.timeout(tableId, jobId, 60, e);
            }

            if (!response.isSuccess()) {
                log.error(
                        "Schema change failed for table: {}, directly failing the job. Error: {}",
                        tableId,
                        response.getMessage());
                throw new SchemaCoordinationException(
                        SchemaEvolutionErrorCode.SCHEMA_CHANGE_COORDINATION_FAILED,
                        "Schema change coordination failed: " + response.getMessage(),
                        tableId,
                        jobId);
            }

            sendSchemaChangeEventToDownstream(schemaChangeEvent);
            updateLocalSchemaState(tableId, schemaChangeEvent.getChangeAfter());

            lastProcessedEventTime = eventTime;
            log.info("Schema change completed successfully for table: {}", tableId);
        } catch (Exception e) {
            log.error("Schema change failed for table: {}, directly failing the job", tableId, e);
            throw e;
        } finally {
            currentSchemaChangeFuture.set(null);
        }
    }

    private void sendFlushEventToDownstream(SchemaChangeEvent schemaChangeEvent) {
        log.info("Send FlushEvent to downstream...");
        FlushEvent flushEvent = new FlushEvent(schemaChangeEvent);

        SeaTunnelRow flushRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("flush_event", flushEvent);
        flushRow.setOptions(options);

        output.collect(new StreamRecord<>(flushRow));
        log.info(
                "FlushEvent sent to downstream for table: {}", schemaChangeEvent.tableIdentifier());
    }

    private void sendSchemaChangeEventToDownstream(SchemaChangeEvent schemaChangeEvent) {
        log.info("Send SchemaChangeEvent to downstream...");

        SeaTunnelRow schemaChangeRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("schema_change_event", schemaChangeEvent);
        schemaChangeRow.setOptions(options);

        output.collect(new StreamRecord<>(schemaChangeRow));
    }

    private void updateLocalSchemaState(TableIdentifier tableId, CatalogTable newSchema) {
        if (newSchema != null) {
            localSchemaState.put(tableId, newSchema);
            log.debug("Updated local schema state for table: {}", tableId);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            CompletableFuture<SchemaResponse> future = currentSchemaChangeFuture.get();
            if (future != null && !future.isDone()) {
                log.info("Cancelling ongoing schema change request during close");
                future.cancel(true);
            }

            if (jobId != null) {
                SchemaCoordinator.removeInstance(jobId);
                log.info("Removed SchemaCoordinator instance for job: {}", jobId);
            }
        } catch (Exception e) {
            log.warn("Error during SchemaOperator cleanup", e);
        } finally {
            super.close();
        }
    }
}
