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

package org.apache.seatunnel.engine.server.task.source;

import java.io.Serializable;

/**
 * Executable main and schema-change state machine.
 *
 * <p>All methods must be called by the managed Source event-loop owner. The class intentionally
 * contains no atomics or synchronization.
 */
public final class ManagedSourceLifecycle {
    private ManagedSourceLifecycleState mainState = ManagedSourceLifecycleState.CREATED;
    private SchemaChangeSubState schemaState = SchemaChangeSubState.IDLE;
    private String schemaPhase = "";
    private long schemaCheckpointId = -1L;
    private long schemaRequestEpoch;
    private long schemaRequestStartedNanos;
    private boolean checkpointBarrierPending;
    private boolean closeLatched;
    private boolean restoreDraining;
    private Throwable failure;

    public void startRestore() {
        requireMain(ManagedSourceLifecycleState.CREATED);
        mainState = ManagedSourceLifecycleState.RESTORING;
    }

    public void finishRestore() {
        requireMain(ManagedSourceLifecycleState.RESTORING);
        schemaState = SchemaChangeSubState.IDLE;
        schemaPhase = "";
        schemaCheckpointId = -1L;
        schemaRequestStartedNanos = 0L;
        checkpointBarrierPending = false;
        mainState =
                restoreDraining
                        ? ManagedSourceLifecycleState.DRAINING
                        : ManagedSourceLifecycleState.RUNNING;
        restoreDraining = false;
    }

    public long beginSchemaChange(String phase, long nowNanos) {
        requireMain(ManagedSourceLifecycleState.RUNNING);
        if (phase == null || phase.trim().isEmpty() || nowNanos <= 0L) {
            throw new IllegalArgumentException(
                    "Managed Source schema phase and start time must be valid");
        }
        if (schemaState != SchemaChangeSubState.IDLE) {
            fail(new IllegalStateException("Overlapping managed Source schema changes"));
            throw failureAsRuntime();
        }
        schemaPhase = phase;
        schemaRequestEpoch++;
        schemaRequestStartedNanos = nowNanos;
        schemaState = SchemaChangeSubState.QUIESCING;
        return schemaRequestEpoch;
    }

    public void schemaTriggerRequested(long requestEpoch) {
        requireSchemaRequest(requestEpoch, SchemaChangeSubState.QUIESCING);
        schemaState = SchemaChangeSubState.TRIGGER_REQUESTED;
    }

    public boolean bindSchemaCheckpoint(String phase, long checkpointId, long requestEpoch) {
        if (checkpointId < 0
                || schemaState != SchemaChangeSubState.TRIGGER_REQUESTED
                || requestEpoch != schemaRequestEpoch
                || !schemaPhase.equals(phase)) {
            return false;
        }
        schemaCheckpointId = checkpointId;
        schemaState = SchemaChangeSubState.WAITING_END;
        return true;
    }

    public boolean checkpointAborted(long checkpointId) {
        if (schemaState == SchemaChangeSubState.WAITING_END && schemaCheckpointId == checkpointId) {
            fail(
                    new IllegalStateException(
                            "Managed Source schema checkpoint " + checkpointId + " was aborted"));
            return true;
        }
        return false;
    }

    public boolean checkpointEnded(long checkpointId) {
        if (schemaState != SchemaChangeSubState.WAITING_END || schemaCheckpointId != checkpointId) {
            return false;
        }
        schemaState = SchemaChangeSubState.IDLE;
        schemaPhase = "";
        schemaCheckpointId = -1L;
        schemaRequestStartedNanos = 0L;
        if (closeLatched) {
            mainState = ManagedSourceLifecycleState.DRAINING;
        }
        return true;
    }

    public void checkSchemaTimeout(long nowNanos, long timeoutNanos) {
        if (timeoutNanos <= 0L) {
            throw new IllegalArgumentException("Managed Source schema timeout must be positive");
        }
        if (schemaState != SchemaChangeSubState.IDLE
                && schemaRequestStartedNanos > 0
                && nowNanos - schemaRequestStartedNanos >= timeoutNanos) {
            fail(
                    new IllegalStateException(
                            "Managed Source schema change timed out in state " + schemaState));
        }
    }

    public void beginCheckpointBarrier(long checkpointId) {
        if (checkpointId < 0L) {
            throw new IllegalArgumentException("Managed Source checkpoint barrier id is invalid");
        }
        if (checkpointBarrierPending) {
            fail(new IllegalStateException("Managed Source checkpoint barrier already pending"));
            throw failureAsRuntime();
        }
        checkpointBarrierPending = true;
    }

    public void finishCheckpointBarrier() {
        checkpointBarrierPending = false;
    }

    public void gracefulClose() {
        if (isTerminal()) {
            return;
        }
        if (mainState == ManagedSourceLifecycleState.CREATED
                || mainState == ManagedSourceLifecycleState.RESTORING) {
            restoreDraining = true;
            closeLatched = true;
            return;
        }
        if (schemaState == SchemaChangeSubState.IDLE) {
            mainState = ManagedSourceLifecycleState.DRAINING;
        } else {
            closeLatched = true;
        }
    }

    public void cancel() {
        if (!isTerminal()) {
            checkpointBarrierPending = false;
            mainState = ManagedSourceLifecycleState.CANCELLING;
        }
    }

    public void fail(Throwable throwable) {
        if (mainState != ManagedSourceLifecycleState.CLOSED) {
            checkpointBarrierPending = false;
            failure = throwable;
            mainState = ManagedSourceLifecycleState.FAILED;
        }
    }

    public void closed() {
        checkpointBarrierPending = false;
        mainState = ManagedSourceLifecycleState.CLOSED;
    }

    public void restoreSnapshot(Snapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        // In-flight schema requests are deliberately fenced on failover.
        mainState = ManagedSourceLifecycleState.RESTORING;
        schemaState = SchemaChangeSubState.IDLE;
        schemaPhase = "";
        schemaCheckpointId = -1L;
        schemaRequestEpoch = Math.max(schemaRequestEpoch, snapshot.schemaRequestEpoch);
        schemaRequestStartedNanos = 0L;
        checkpointBarrierPending = false;
        closeLatched = snapshot.closeLatched;
        restoreDraining =
                snapshot.mainState == ManagedSourceLifecycleState.DRAINING || snapshot.closeLatched;
        failure = null;
    }

    public Snapshot snapshot() {
        return new Snapshot(
                mainState,
                schemaState,
                schemaPhase,
                schemaCheckpointId,
                schemaRequestEpoch,
                closeLatched);
    }

    public boolean canPoll() {
        return mainState == ManagedSourceLifecycleState.RUNNING
                && schemaState == SchemaChangeSubState.IDLE
                && !checkpointBarrierPending;
    }

    public boolean isDraining() {
        return mainState == ManagedSourceLifecycleState.DRAINING;
    }

    public boolean isFailed() {
        return mainState == ManagedSourceLifecycleState.FAILED;
    }

    public boolean isTerminal() {
        return mainState == ManagedSourceLifecycleState.FAILED
                || mainState == ManagedSourceLifecycleState.CANCELLING
                || mainState == ManagedSourceLifecycleState.CLOSED;
    }

    public Throwable getFailure() {
        return failure;
    }

    public ManagedSourceLifecycleState getMainState() {
        return mainState;
    }

    public SchemaChangeSubState getSchemaState() {
        return schemaState;
    }

    public long getSchemaRequestEpoch() {
        return schemaRequestEpoch;
    }

    public String getSchemaPhase() {
        return schemaPhase;
    }

    public boolean isCheckpointBarrierPending() {
        return checkpointBarrierPending;
    }

    private void requireMain(ManagedSourceLifecycleState expected) {
        if (mainState != expected) {
            throw new IllegalStateException(
                    "Expected managed Source state " + expected + " but was " + mainState);
        }
    }

    private void requireSchemaRequest(long requestEpoch, SchemaChangeSubState expected) {
        if (requestEpoch != schemaRequestEpoch || schemaState != expected) {
            throw new IllegalStateException(
                    "Stale managed Source schema request "
                            + requestEpoch
                            + ", current="
                            + schemaRequestEpoch
                            + "/"
                            + schemaState);
        }
    }

    private RuntimeException failureAsRuntime() {
        return failure instanceof RuntimeException
                ? (RuntimeException) failure
                : new IllegalStateException(failure);
    }

    /** Serializable state-machine diagnostics persisted without in-flight futures. */
    public static final class Snapshot implements Serializable {
        private static final long serialVersionUID = 1L;

        private final ManagedSourceLifecycleState mainState;
        private final SchemaChangeSubState schemaState;
        private final String schemaPhase;
        private final long schemaCheckpointId;
        private final long schemaRequestEpoch;
        private final boolean closeLatched;

        public Snapshot(
                ManagedSourceLifecycleState mainState,
                SchemaChangeSubState schemaState,
                String schemaPhase,
                long schemaCheckpointId,
                long schemaRequestEpoch,
                boolean closeLatched) {
            if (mainState == null
                    || schemaState == null
                    || schemaPhase == null
                    || schemaRequestEpoch < 0
                    || mainState == ManagedSourceLifecycleState.CREATED
                    || mainState == ManagedSourceLifecycleState.CANCELLING
                    || mainState == ManagedSourceLifecycleState.FAILED
                    || mainState == ManagedSourceLifecycleState.CLOSED) {
                throw new IllegalArgumentException(
                        "Managed Source lifecycle snapshot metadata is invalid");
            }
            boolean idle = schemaState == SchemaChangeSubState.IDLE;
            boolean waitingEnd = schemaState == SchemaChangeSubState.WAITING_END;
            if ((idle && (!schemaPhase.isEmpty() || schemaCheckpointId != -1L))
                    || (!idle && schemaPhase.trim().isEmpty())
                    || (waitingEnd && schemaCheckpointId < 0L)
                    || (!waitingEnd && schemaCheckpointId != -1L)
                    || (!idle && mainState != ManagedSourceLifecycleState.RUNNING)
                    || (mainState == ManagedSourceLifecycleState.DRAINING && !idle)) {
                throw new IllegalArgumentException(
                        "Managed Source lifecycle snapshot transition is inconsistent");
            }
            this.mainState = mainState;
            this.schemaState = schemaState;
            this.schemaPhase = schemaPhase;
            this.schemaCheckpointId = schemaCheckpointId;
            this.schemaRequestEpoch = schemaRequestEpoch;
            this.closeLatched = closeLatched;
        }

        public ManagedSourceLifecycleState getMainState() {
            return mainState;
        }

        public SchemaChangeSubState getSchemaState() {
            return schemaState;
        }

        public String getSchemaPhase() {
            return schemaPhase;
        }

        public long getSchemaCheckpointId() {
            return schemaCheckpointId;
        }

        public long getSchemaRequestEpoch() {
            return schemaRequestEpoch;
        }

        public boolean isCloseLatched() {
            return closeLatched;
        }
    }
}
