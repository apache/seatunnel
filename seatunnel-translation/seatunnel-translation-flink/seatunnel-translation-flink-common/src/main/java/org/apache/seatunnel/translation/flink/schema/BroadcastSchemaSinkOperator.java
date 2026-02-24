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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaCoordinationException;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.schema.exception.SchemaValidationException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.coordinator.LocalSchemaCoordinator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * BroadcastSchemaSinkOperator is a Flink operator that coordinates schema changes across parallel
 * sink subtasks using immediate application
 */
@Slf4j
public class BroadcastSchemaSinkOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {

    private transient Map<TableIdentifier, Long> lastProcessedEpoch;
    private transient ListState<TableEpochEntry> lastProcessedEpochState;
    private transient LocalSchemaCoordinator coordinator;
    private String jobId;

    @Getter
    @Setter
    public static class TableEpochEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private TableIdentifier tableId = null;
        private long epoch = 0L;

        public TableEpochEntry() {}

        public TableEpochEntry(TableIdentifier tableId, long epoch) {
            this.tableId = tableId;
            this.epoch = epoch;
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        ListStateDescriptor<TableEpochEntry> epochDescriptor =
                new ListStateDescriptor<>("last-processed-epochs", TableEpochEntry.class);
        lastProcessedEpochState = context.getOperatorStateStore().getListState(epochDescriptor);

        this.lastProcessedEpoch = new HashMap<>();

        if (context.isRestored()) {
            for (TableEpochEntry entry : lastProcessedEpochState.get()) {
                lastProcessedEpoch.put(entry.tableId, entry.epoch);
                log.info(
                        "Restored last processed epoch {} for table {}",
                        entry.epoch,
                        entry.tableId);
            }
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

        this.jobId = getRuntimeContext().getJobId().toString();
        this.coordinator = LocalSchemaCoordinator.getInstance(jobId);

        if (subtaskId == 0) {
            coordinator.registerSinkParallelism(parallelism);
        }

        // register this subtask as a state provider for the coordinator
        coordinator.registerSinkStateProvider(
                subtaskId, tableId -> lastProcessedEpoch.get(tableId));
        log.info("BroadcastSchemaSinkOperator opened on subtask {}/{}", subtaskId, parallelism);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        lastProcessedEpochState.clear();
        for (Map.Entry<TableIdentifier, Long> entry : lastProcessedEpoch.entrySet()) {
            lastProcessedEpochState.add(new TableEpochEntry(entry.getKey(), entry.getValue()));
        }

        log.debug(
                "Subtask {} snapshotted state with last processed epochs for {} tables",
                getRuntimeContext().getIndexOfThisSubtask(),
                lastProcessedEpoch.size());
    }

    @Override
    public void processElement(StreamRecord<SeaTunnelRow> element) throws Exception {
        SeaTunnelRow row = element.getValue();
        Map<String, Object> options = row.getOptions();

        if (options != null && options.containsKey("schema_change_broadcast")) {
            SchemaChangeEvent event = (SchemaChangeEvent) options.get("schema_change_broadcast");
            handleBroadcastedSchemaChange(event);
            return;
        }

        output.collect(element);
    }

    private void handleBroadcastedSchemaChange(SchemaChangeEvent event) {
        TableIdentifier tableId = event.tableIdentifier();
        long epoch = event.getCreatedTime();
        try {
            Long lastEpoch = lastProcessedEpoch.get(tableId);
            if (lastEpoch != null && epoch <= lastEpoch) {
                log.info(
                        "Subtask {} already processed schema change for table {} (epoch {}), last processed: {}. "
                                + "Sending ACK to coordinator for this duplicate event.",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        tableId,
                        epoch,
                        lastEpoch);

                // send ACK for this already-processed event to avoid coordinator timeout
                coordinator.notifySchemaChangeApplied(
                        tableId, epoch, getRuntimeContext().getIndexOfThisSubtask(), true);
                return;
            }
            int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
            log.info(
                    "Subtask {} applying schema change immediately for table {} (epoch {}, change: {}). This prevents deadlock by allowing checkpoint barriers to propagate.",
                    subtaskId,
                    tableId,
                    epoch,
                    event.getClass().getSimpleName());

            try {
                emitApplySchemaEventToSink(event, epoch);
                lastProcessedEpoch.put(tableId, epoch);

                // send ACK to coordinator indicating this subtask has processed the schema change
                coordinator.notifySchemaChangeApplied(tableId, epoch, subtaskId, true);

                log.info(
                        "Subtask {} processed schema change for table {} (epoch {}) and sent ACK to coordinator.",
                        subtaskId,
                        tableId,
                        epoch);
            } catch (Exception e) {
                coordinator.notifySchemaChangeApplied(tableId, epoch, subtaskId, false);
                throw e;
            }
        } catch (SchemaValidationException | SchemaCoordinationException e) {
            log.error("Schema broadcast or coordination error", e);
            throw e;
        } catch (Exception e) {
            log.error("Schema change dispatch failed", e);
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    e.getMessage(),
                    tableId,
                    jobId,
                    e);
        }
    }

    private void emitApplySchemaEventToSink(SchemaChangeEvent event, long epoch) {
        SeaTunnelRow schemaRow = new SeaTunnelRow(0);
        Map<String, Object> opts = new HashMap<>();
        opts.put("schema_change_event", event);
        opts.put("schema_epoch", epoch);
        opts.put("schema_subtask_id", (long) getRuntimeContext().getIndexOfThisSubtask());
        schemaRow.setOptions(opts);

        output.collect(new StreamRecord<>(schemaRow));

        log.debug(
                "Subtask {} emitted schema change event for table {}",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.tableIdentifier());
    }

    @Override
    public void close() throws Exception {
        super.close();
        log.info(
                "BroadcastSchemaSinkOperator closed on subtask {}",
                getRuntimeContext().getIndexOfThisSubtask());
    }
}
