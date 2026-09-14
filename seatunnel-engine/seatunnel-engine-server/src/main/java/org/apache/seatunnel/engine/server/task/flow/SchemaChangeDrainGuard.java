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
     * Default bound on how long {@link #checkSchemaChangeCanApply} waits for a schema-change-before
     * checkpoint completion that may still be in flight.
     *
     * <p>The checkpoint coordinator's completion notification fans out to the source and sink tasks
     * in parallel with no ordering guarantee between them, so the source's real {@link
     * SchemaChangeEvent} can reach this sink before this guard's own completion callback has run.
     * That gap is normally sub-millisecond; a wait this long only matters if the checkpoint
     * genuinely never completes, which is a real protocol violation this guard must still reject.
     */
    private static final long DRAIN_READY_WAIT_TIMEOUT_MILLIS = 30_000L;

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

    /** Bound actually applied by {@link #checkSchemaChangeCanApply}; overridable for tests. */
    private final long drainReadyWaitTimeoutMillis;

    SchemaChangeDrainGuard() {
        this(DRAIN_READY_WAIT_TIMEOUT_MILLIS);
    }

    /**
     * @param drainReadyWaitTimeoutMillis bound used in place of {@link
     *     #DRAIN_READY_WAIT_TIMEOUT_MILLIS}; package-private so tests can keep the parallel-notify
     *     race deterministic and fast without waiting out the full production timeout.
     */
    SchemaChangeDrainGuard(long drainReadyWaitTimeoutMillis) {
        this.drainReadyWaitTimeoutMillis = drainReadyWaitTimeoutMillis;
    }

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
     * <p>On recovery this is the only call that can reopen a freshly constructed guard (a new
     * {@code SinkFlowLifeCycle}, and therefore a new {@code SchemaChangeDrainGuard} with {@code
     * schemaChangeDrainReady=false}, is created whenever the sink task restarts). It is driven by
     * {@code CheckpointCoordinator.allTaskReady()} replaying {@code latestCompletedCheckpoint}
     * through {@code notifyCompleted()} once every subtask in the pipeline reports {@code
     * READY_START}. Since the pipeline's {@code CheckpointCoordinator} is always recreated together
     * with a restarting task (task failure recovery is pipeline-scoped, not per-task, in this
     * engine -- see {@code SubPlan.cancelPipeline()}/{@code reset()}), {@code isAllTaskReady}'s
     * once-per-coordinator guard can never suppress this replay for a task that actually needs it.
     *
     * @param checkpointId completed checkpoint id reported by the checkpoint coordinator
     * @param checkpointType completed checkpoint type reported by the checkpoint coordinator
     */
    synchronized void checkpointCompleted(long checkpointId, CheckpointType checkpointType) {
        if (isSchemaChangeBeforeCheckpoint(checkpointId, checkpointType)) {
            schemaChangeBeforeCheckpointId = checkpointId;
            schemaChangeDrainReady = true;
            // Wake any checkSchemaChangeCanApply call already waiting on this exact race: the
            // source's SchemaChangeEvent reached this sink before this completion notification.
            notifyAll();
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
     * Waits for the schema-change-before checkpoint to complete before a sink applies DDL, then
     * fails if it never does.
     *
     * <p>The checkpoint coordinator notifies the source and this sink of completion in parallel
     * with no ordering guarantee, so the source's event can arrive here just ahead of this guard's
     * own {@link #checkpointCompleted} callback. Waits up to {@link
     * #DRAIN_READY_WAIT_TIMEOUT_MILLIS} for that in-flight notification before rejecting the event
     * as a genuine protocol violation.
     *
     * @param event schema change event that is about to be applied by a sink writer
     */
    synchronized void checkSchemaChangeCanApply(SchemaChangeEvent event) {
        long deadline = System.currentTimeMillis() + drainReadyWaitTimeoutMillis;
        while (!schemaChangeDrainReady) {
            long remainingMillis = deadline - System.currentTimeMillis();
            if (remainingMillis <= 0) {
                throw new IllegalStateException(
                        String.format(
                                "Schema change event [%s] for table [%s] cannot be applied before a "
                                        + "schema-change-before checkpoint is completed. Sources must "
                                        + "call Collector.markSchemaChangeBeforeCheckpoint() and wait "
                                        + "for the checkpoint to finish before emitting SchemaChangeEvent.",
                                event.getEventType(), event.tablePath()));
            }
            try {
                wait(remainingMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        String.format(
                                "Interrupted while waiting for the schema-change-before checkpoint "
                                        + "to complete for event [%s] on table [%s]",
                                event.getEventType(), event.tablePath()),
                        e);
            }
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
