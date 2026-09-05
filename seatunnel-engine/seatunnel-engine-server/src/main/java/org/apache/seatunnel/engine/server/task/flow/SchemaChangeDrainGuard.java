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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.record.Barrier;

/**
 * Guards sink-side DDL application with the schema-change checkpoint protocol.
 *
 * <p>A schema-change-before checkpoint must be completed before a sink writer can apply a {@link
 * SchemaChangeEvent}. This keeps old-schema rows behind a global checkpoint boundary instead of
 * relying on each sink connector to rediscover the same drain requirement.
 */
class SchemaChangeDrainGuard {

    /**
     * Sentinel used when no schema-change checkpoint is currently tracked.
     *
     * <p>Checkpoint ids are non-negative in normal execution, so this value cannot collide with a
     * coordinator-assigned checkpoint id.
     */
    private static final long UNKNOWN_CHECKPOINT_ID = -1L;

    /**
     * Latest schema-change-before checkpoint barrier handled by this sink subtask.
     *
     * <p>The id is used for non-recovery paths where the sink has observed the barrier before the
     * completion notification arrives.
     */
    private long schemaChangeBeforeCheckpointId = UNKNOWN_CHECKPOINT_ID;

    /**
     * Latest schema-change-after checkpoint barrier handled by this sink subtask.
     *
     * <p>The id closes the DDL window for normal runtime notifications that do not carry a
     * checkpoint type.
     */
    private long schemaChangeAfterCheckpointId = UNKNOWN_CHECKPOINT_ID;

    /**
     * Whether a completed schema-change-before checkpoint currently protects sink-side schema
     * changes.
     */
    private boolean schemaChangeDrainReady;

    /**
     * Records schema-change checkpoint ids only after the sink has handled the barrier
     * successfully.
     *
     * @param barrier checkpoint barrier propagated to this sink subtask
     */
    synchronized void checkpointBarrierHandled(Barrier barrier) {
        if (!(barrier instanceof CheckpointBarrier)) {
            return;
        }
        CheckpointBarrier checkpointBarrier = (CheckpointBarrier) barrier;
        CheckpointType checkpointType = checkpointBarrier.getCheckpointType();
        if (checkpointType.isSchemaChangeBeforeCheckpoint()) {
            boolean alreadyCompleted =
                    schemaChangeDrainReady
                            && checkpointBarrier.getId() == schemaChangeBeforeCheckpointId;
            schemaChangeBeforeCheckpointId = checkpointBarrier.getId();
            schemaChangeDrainReady = alreadyCompleted;
        } else if (checkpointType.isSchemaChangeAfterCheckpoint()) {
            schemaChangeAfterCheckpointId = checkpointBarrier.getId();
        }
    }

    /**
     * Opens or closes the schema-change DDL window when a tracked checkpoint is globally completed.
     *
     * @param checkpointId completed checkpoint id reported by the checkpoint coordinator
     */
    synchronized void checkpointCompleted(long checkpointId) {
        checkpointCompleted(checkpointId, null);
    }

    /**
     * Opens or closes the schema-change DDL window when a typed checkpoint is globally completed.
     *
     * <p>The checkpoint type is required after failover because a sink can be restored from a
     * completed schema-change-before checkpoint without observing that barrier in the new runtime.
     *
     * @param checkpointId completed checkpoint id reported by the checkpoint coordinator
     * @param checkpointType completed checkpoint type reported by the checkpoint coordinator
     */
    synchronized void checkpointCompleted(long checkpointId, CheckpointType checkpointType) {
        if (isSchemaChangeBeforeCheckpoint(checkpointId, checkpointType)) {
            schemaChangeBeforeCheckpointId = checkpointId;
            schemaChangeDrainReady = true;
        } else if (isSchemaChangeAfterCheckpoint(checkpointId, checkpointType)) {
            schemaChangeAfterCheckpointId = checkpointId;
            reset();
        }
    }

    /**
     * Clears tracked schema-change checkpoint state when the coordinator aborts the checkpoint.
     *
     * @param checkpointId aborted checkpoint id reported by the checkpoint coordinator
     */
    synchronized void checkpointAborted(long checkpointId) {
        checkpointAborted(checkpointId, null);
    }

    /**
     * Clears tracked schema-change checkpoint state when a typed checkpoint is aborted.
     *
     * @param checkpointId aborted checkpoint id reported by the checkpoint coordinator
     * @param checkpointType aborted checkpoint type reported by the checkpoint coordinator
     */
    synchronized void checkpointAborted(long checkpointId, CheckpointType checkpointType) {
        if (checkpointId == schemaChangeBeforeCheckpointId
                || checkpointId == schemaChangeAfterCheckpointId
                || (checkpointType != null && checkpointType.isSchemaChangeCheckpoint())) {
            reset();
        }
    }

    /**
     * Fails fast when a sink tries to apply DDL before the schema-change-before checkpoint
     * completes.
     *
     * @param event schema change event that is about to be applied by a sink writer
     */
    synchronized void checkSchemaChangeCanApply(SchemaChangeEvent event) {
        if (!schemaChangeDrainReady) {
            throw new IllegalStateException(
                    String.format(
                            "Schema change event [%s] for table [%s] cannot be applied before a "
                                    + "schema-change-before checkpoint is completed. Sources must "
                                    + "call Collector.markSchemaChangeBeforeCheckpoint() and wait "
                                    + "for the checkpoint to finish before emitting SchemaChangeEvent.",
                            event.getEventType(), event.tablePath()));
        }
    }

    /**
     * Returns whether sink-side schema changes are currently protected by a completed drain
     * checkpoint.
     *
     * @return true when schema changes can be applied safely
     */
    synchronized boolean isSchemaChangeDrainReady() {
        return schemaChangeDrainReady;
    }

    /**
     * Checks whether a checkpoint id or type represents a schema-change-before checkpoint.
     *
     * @param checkpointId checkpoint id reported by the coordinator
     * @param checkpointType checkpoint type reported by the coordinator, or null for legacy callers
     * @return true when the checkpoint opens the sink DDL window
     */
    private boolean isSchemaChangeBeforeCheckpoint(
            long checkpointId, CheckpointType checkpointType) {
        return checkpointType == null
                ? checkpointId == schemaChangeBeforeCheckpointId
                : checkpointType.isSchemaChangeBeforeCheckpoint();
    }

    /**
     * Checks whether a checkpoint id or type represents a schema-change-after checkpoint.
     *
     * @param checkpointId checkpoint id reported by the coordinator
     * @param checkpointType checkpoint type reported by the coordinator, or null for legacy callers
     * @return true when the checkpoint closes the sink DDL window
     */
    private boolean isSchemaChangeAfterCheckpoint(
            long checkpointId, CheckpointType checkpointType) {
        return checkpointType == null
                ? checkpointId == schemaChangeAfterCheckpointId
                : checkpointType.isSchemaChangeAfterCheckpoint();
    }

    /**
     * Resets checkpoint ids and closes the tracked schema-change checkpoint window.
     *
     * <p>After reset, a new schema-change-before checkpoint must complete before sink DDL can run.
     */
    private void reset() {
        schemaChangeBeforeCheckpointId = UNKNOWN_CHECKPOINT_ID;
        schemaChangeAfterCheckpointId = UNKNOWN_CHECKPOINT_ID;
        schemaChangeDrainReady = false;
    }
}
