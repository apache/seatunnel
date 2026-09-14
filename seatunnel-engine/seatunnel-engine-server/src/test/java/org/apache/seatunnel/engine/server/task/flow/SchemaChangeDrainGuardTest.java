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

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Covers the sink-side schema-change drain contract before DDL is applied by sink writers.
 *
 * <p>The cases verify both normal barrier ordering and recovery from a completed
 * schema-change-before checkpoint.
 */
class SchemaChangeDrainGuardTest {

    /**
     * Short bound for cases where the checkpoint that would open the drain window is never
     * completed: checkSchemaChangeCanApply now waits (to absorb the checkpoint coordinator's
     * parallel source/sink completion-notification race) before rejecting, so these cases use a
     * short timeout to stay fast and deterministic instead of waiting out the full production
     * default.
     */
    private static final long TEST_DRAIN_READY_WAIT_TIMEOUT_MILLIS = 50L;

    /**
     * Verifies that a schema change cannot bypass the schema-change-before checkpoint protocol.
     *
     * <p>This protects connectors from applying DDL while old-schema rows may still be buffered.
     */
    @Test
    void shouldRejectSchemaChangeBeforeDrainCheckpointCompletes() {
        SchemaChangeDrainGuard guard =
                new SchemaChangeDrainGuard(TEST_DRAIN_READY_WAIT_TIMEOUT_MILLIS);

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));

        Assertions.assertTrue(
                exception.getMessage().contains("schema-change-before checkpoint is completed"));
    }

    /**
     * Verifies that seeing a schema-change-before barrier is not enough until the checkpoint
     * coordinator reports completion.
     */
    @Test
    void shouldRejectSchemaChangeAfterBarrierButBeforeCheckpointCompletion() {
        SchemaChangeDrainGuard guard =
                new SchemaChangeDrainGuard(TEST_DRAIN_READY_WAIT_TIMEOUT_MILLIS);

        guard.checkpointBarrierHandled(schemaChangeBeforeBarrier(1L));

        Assertions.assertFalse(guard.isSchemaChangeDrainReady());
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));
    }

    /**
     * Verifies that a completed schema-change-before checkpoint opens the safe DDL application
     * window.
     */
    @Test
    void shouldAllowSchemaChangeAfterDrainCheckpointCompletes() {
        SchemaChangeDrainGuard guard = new SchemaChangeDrainGuard();

        guard.checkpointBarrierHandled(schemaChangeBeforeBarrier(1L));
        guard.checkpointCompleted(1L);

        Assertions.assertTrue(guard.isSchemaChangeDrainReady());
        Assertions.assertDoesNotThrow(() -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));
    }

    /**
     * Verifies that a restored sink can apply DDL after the coordinator reports a completed
     * schema-change-before checkpoint type.
     */
    @Test
    void shouldAllowSchemaChangeAfterRestoredDrainCheckpointCompletes() {
        SchemaChangeDrainGuard guard = new SchemaChangeDrainGuard();

        guard.checkpointCompleted(1L, CheckpointType.SCHEMA_CHANGE_BEFORE_POINT_TYPE);

        Assertions.assertTrue(guard.isSchemaChangeDrainReady());
        Assertions.assertDoesNotThrow(() -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));
    }

    /**
     * Verifies that a typed completion notification cannot be undone when the same barrier is
     * recorded later in the sink lifecycle.
     *
     * <p>This protects the sink path from callback ordering around local ACK and checkpoint-finish
     * notification delivery.
     */
    @Test
    void shouldKeepDrainReadyWhenSameBeforeBarrierIsRecordedAfterTypedCompletion() {
        SchemaChangeDrainGuard guard = new SchemaChangeDrainGuard();

        guard.checkpointCompleted(1L, CheckpointType.SCHEMA_CHANGE_BEFORE_POINT_TYPE);
        guard.checkpointBarrierHandled(schemaChangeBeforeBarrier(1L));

        Assertions.assertTrue(guard.isSchemaChangeDrainReady());
        Assertions.assertDoesNotThrow(() -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));
    }

    /**
     * Verifies that the schema-change-after checkpoint closes the DDL application window after the
     * schema change phase has finished.
     */
    @Test
    void shouldCloseDrainWindowAfterSchemaChangeAfterCheckpointCompletes() {
        SchemaChangeDrainGuard guard =
                new SchemaChangeDrainGuard(TEST_DRAIN_READY_WAIT_TIMEOUT_MILLIS);

        guard.checkpointBarrierHandled(schemaChangeBeforeBarrier(1L));
        guard.checkpointCompleted(1L);
        guard.checkpointBarrierHandled(schemaChangeAfterBarrier(2L));
        guard.checkpointCompleted(2L);

        Assertions.assertFalse(guard.isSchemaChangeDrainReady());
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));
    }

    /**
     * Verifies that a typed schema-change-after checkpoint closes the DDL window after recovery.
     *
     * <p>This covers the case where the restored sink did not observe the original after barrier.
     */
    @Test
    void shouldCloseRestoredDrainWindowAfterSchemaChangeAfterCheckpointCompletes() {
        SchemaChangeDrainGuard guard =
                new SchemaChangeDrainGuard(TEST_DRAIN_READY_WAIT_TIMEOUT_MILLIS);

        guard.checkpointCompleted(1L, CheckpointType.SCHEMA_CHANGE_BEFORE_POINT_TYPE);
        guard.checkpointCompleted(2L, CheckpointType.SCHEMA_CHANGE_AFTER_POINT_TYPE);

        Assertions.assertFalse(guard.isSchemaChangeDrainReady());
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));
    }

    /**
     * Verifies that an aborted schema-change-after checkpoint cannot leave a stale DDL permission.
     */
    @Test
    void shouldClearDrainStateWhenSchemaChangeCheckpointIsAborted() {
        SchemaChangeDrainGuard guard =
                new SchemaChangeDrainGuard(TEST_DRAIN_READY_WAIT_TIMEOUT_MILLIS);

        guard.checkpointBarrierHandled(schemaChangeBeforeBarrier(1L));
        guard.checkpointCompleted(1L);
        guard.checkpointBarrierHandled(schemaChangeAfterBarrier(2L));
        guard.checkpointAborted(2L);

        Assertions.assertFalse(guard.isSchemaChangeDrainReady());
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> guard.checkSchemaChangeCanApply(schemaChangeEvent()));
    }

    /**
     * Proves the parallel-notification race this guard now absorbs: the checkpoint coordinator's
     * completion notification fans out to the source and sink tasks in parallel with no ordering
     * guarantee, so the source's real event can reach this sink just before this guard's own
     * completion callback runs on another thread. checkSchemaChangeCanApply must wait for that
     * in-flight callback and succeed once it arrives, rather than rejecting a genuinely valid
     * schema change.
     */
    @Test
    void shouldAllowSchemaChangeWhenCompletionArrivesWhileWaiting() throws InterruptedException {
        SchemaChangeDrainGuard guard = new SchemaChangeDrainGuard(TimeUnit.SECONDS.toMillis(5));
        CountDownLatch waitingStarted = new CountDownLatch(1);
        AtomicReference<Throwable> applyFailure = new AtomicReference<>();

        Thread applyThread =
                new Thread(
                        () -> {
                            waitingStarted.countDown();
                            try {
                                guard.checkSchemaChangeCanApply(schemaChangeEvent());
                            } catch (Throwable e) {
                                applyFailure.set(e);
                            }
                        });
        applyThread.start();
        Assertions.assertTrue(waitingStarted.await(5, TimeUnit.SECONDS));
        // Give the apply thread a moment to actually enter the wait before completing the
        // checkpoint from this thread, simulating the coordinator's parallel source/sink
        // notification race.
        Thread.sleep(100L);

        guard.checkpointCompleted(1L, CheckpointType.SCHEMA_CHANGE_BEFORE_POINT_TYPE);
        applyThread.join(TimeUnit.SECONDS.toMillis(5));

        Assertions.assertFalse(applyThread.isAlive(), "apply thread must unblock once notified");
        Assertions.assertNull(
                applyFailure.get(),
                "a genuinely completed checkpoint must not reject the schema change");
    }

    /**
     * Builds the schema-change-before barrier used by the sink drain guard.
     *
     * @param checkpointId schema-change-before checkpoint id
     * @return checkpoint barrier with schema-change-before type
     */
    private static CheckpointBarrier schemaChangeBeforeBarrier(long checkpointId) {
        return new CheckpointBarrier(
                checkpointId,
                System.currentTimeMillis(),
                CheckpointType.SCHEMA_CHANGE_BEFORE_POINT_TYPE);
    }

    /**
     * Builds the schema-change-after barrier used by the sink drain guard.
     *
     * @param checkpointId schema-change-after checkpoint id
     * @return checkpoint barrier with schema-change-after type
     */
    private static CheckpointBarrier schemaChangeAfterBarrier(long checkpointId) {
        return new CheckpointBarrier(
                checkpointId,
                System.currentTimeMillis(),
                CheckpointType.SCHEMA_CHANGE_AFTER_POINT_TYPE);
    }

    /**
     * Builds a representative alter-table event that requires the drain guard.
     *
     * @return schema change event used in guard tests
     */
    private static SchemaChangeEvent schemaChangeEvent() {
        return AlterTableAddColumnEvent.add(
                TableIdentifier.of("", "test_db", "test_table"),
                PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, null));
    }
}
