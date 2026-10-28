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

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelSourceCollector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Protects the handoff counter and its real source lifecycle barrier/timer wiring.
 *
 * <p>Tests use actual collector monitors and controlled poll/injection boundaries so starvation,
 * premature reader resumption, unbalanced failure cleanup, and cancellation each fail directly.
 */
public class SourceCheckpointLockHandoffTest {

    private static final long JOIN_TIMEOUT_MS = 10_000L;

    @Test
    public void awaitReturnsImmediatelyWhenNoInjectorIsPending() throws InterruptedException {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        Assertions.assertFalse(handoff.hasPendingInjector());
        Assertions.assertEquals(0L, handoff.awaitInjectors());
    }

    @Test
    public void fastPathHonoursInterruptWithoutPendingInjector() {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        try {
            Thread.currentThread().interrupt();
            Assertions.assertThrows(InterruptedException.class, handoff::awaitInjectors);
            Assertions.assertFalse(Thread.currentThread().isInterrupted());
        } finally {
            // Keep the test runner healthy even when the assertion detects a regression.
            Thread.interrupted();
        }
    }

    @Test
    public void pendingStateTracksEveryInjectorIndependently() {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        handoff.injectorArriving();
        handoff.injectorArriving();
        Assertions.assertTrue(handoff.hasPendingInjector());
        handoff.injectorFinished();
        Assertions.assertTrue(handoff.hasPendingInjector());
        handoff.injectorFinished();
        Assertions.assertFalse(handoff.hasPendingInjector());
    }

    @Test
    public void injectorAcquiresLockWithinOnePollCycle() throws Exception {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        Object checkpointLock = new Object();
        AtomicInteger completedPolls = new AtomicInteger();
        AtomicBoolean readerRunning = new AtomicBoolean(true);
        CountDownLatch readerHoldsLock = new CountDownLatch(1);
        CountDownLatch releasePoll = new CountDownLatch(1);
        AtomicInteger pollsBeforeInjector = new AtomicInteger(-1);
        AtomicInteger pollsWhenInjectorAcquired = new AtomicInteger(-1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader =
                thread(
                        "handoff-reader",
                        failure,
                        () -> {
                            while (readerRunning.get()) {
                                synchronized (checkpointLock) {
                                    readerHoldsLock.countDown();
                                    Assertions.assertTrue(
                                            releasePoll.await(
                                                    JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS));
                                }
                                completedPolls.incrementAndGet();
                                handoff.awaitInjectors();
                            }
                        });
        Thread injector =
                thread(
                        "handoff-injector",
                        failure,
                        () -> {
                            handoff.injectorArriving();
                            try {
                                // Announce before sampling. Preemption before registration must not
                                // charge
                                // unrelated reader polls against the handoff's cross-poll bound.
                                pollsBeforeInjector.set(completedPolls.get());
                                synchronized (checkpointLock) {
                                    pollsWhenInjectorAcquired.set(completedPolls.get());
                                }
                            } finally {
                                handoff.injectorFinished();
                            }
                        });
        try {
            reader.start();
            Assertions.assertTrue(readerHoldsLock.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS));
            injector.start();
            awaitThreadState(injector, Thread.State.BLOCKED, failure);
            releasePoll.countDown();
            injector.join(JOIN_TIMEOUT_MS);
            Assertions.assertFalse(injector.isAlive(), "injector did not acquire the monitor");
            Assertions.assertNull(failure.get());
            int pollsWaited = pollsWhenInjectorAcquired.get() - pollsBeforeInjector.get();
            Assertions.assertTrue(
                    pollsWaited >= 0 && pollsWaited <= 1,
                    "injector waited " + pollsWaited + " poll cycles");
            Assertions.assertFalse(handoff.hasPendingInjector());
        } finally {
            readerRunning.set(false);
            releasePoll.countDown();
            stopThreads(reader, injector);
        }
    }

    @Test
    public void waitingReaderResumesWhenInjectorFinishes() throws Exception {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        handoff.injectorArriving();
        AtomicBoolean resumed = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader =
                thread(
                        "waiting-reader",
                        failure,
                        () -> {
                            handoff.awaitInjectors();
                            resumed.set(true);
                        });
        boolean registered = true;
        try {
            reader.start();
            awaitThreadState(reader, Thread.State.TIMED_WAITING, failure);
            Assertions.assertFalse(resumed.get());
            handoff.injectorFinished();
            registered = false;
            reader.join(JOIN_TIMEOUT_MS);
            Assertions.assertFalse(reader.isAlive());
            Assertions.assertTrue(resumed.get());
            Assertions.assertNull(failure.get());
        } finally {
            if (registered) {
                handoff.injectorFinished();
            }
            stopThreads(reader);
        }
    }

    @Test
    public void waitingReaderHonoursInterrupt() throws Exception {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        handoff.injectorArriving();
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader =
                thread(
                        "interrupted-reader",
                        failure,
                        () -> {
                            try {
                                handoff.awaitInjectors();
                            } catch (InterruptedException e) {
                                interrupted.set(true);
                            }
                        });
        try {
            reader.start();
            awaitThreadState(reader, Thread.State.TIMED_WAITING, failure);
            reader.interrupt();
            reader.join(JOIN_TIMEOUT_MS);
            Assertions.assertFalse(reader.isAlive());
            Assertions.assertTrue(interrupted.get());
            Assertions.assertNull(failure.get());
        } finally {
            handoff.injectorFinished();
            stopThreads(reader);
        }
    }

    @Test
    public void lifecycleCollectHandsOffToBarrierBeforeReturning() throws Exception {
        assertLifecycleHandoff(false);
    }

    @Test
    public void lifecycleCollectHandsOffToTimerBeforeReturning() throws Exception {
        assertLifecycleHandoff(true);
    }

    @Test
    public void lifecycleFailuresReleaseBarrierAndTimerRegistrations() throws Exception {
        for (String failurePoint : Arrays.asList("snapshot", "barrier-forward", "flush-forward")) {
            LifecycleFixture fixture = new LifecycleFixture();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread reader = thread("reader-after-failed-injection", failure, fixture.flow::collect);
            try {
                fixture.releasePoll.countDown();
                fixture.releaseInjection.countDown();
                if ("snapshot".equals(failurePoint)) {
                    Mockito.when(fixture.reader.snapshotState(11L))
                            .thenThrow(new IOException("snapshot failure"));
                    Assertions.assertThrows(
                            IOException.class, () -> fixture.flow.triggerBarrier(fixture.barrier));
                } else {
                    fixture.failForward.set(true);
                    if ("flush-forward".equals(failurePoint)) {
                        fixture.timerTick.get().run();
                    } else {
                        Assertions.assertThrows(
                                IOException.class,
                                () -> fixture.flow.triggerBarrier(fixture.barrier));
                    }
                }
                fixture.failForward.set(false);
                reader.start();
                reader.join(JOIN_TIMEOUT_MS);
                Assertions.assertFalse(reader.isAlive(), failurePoint + " left the reader parked");
                Assertions.assertNull(failure.get());
                Assertions.assertTrue(
                        fixture.records.get(fixture.records.size() - 1).getData()
                                instanceof SeaTunnelRow);
            } finally {
                fixture.releasePoll.countDown();
                fixture.releaseInjection.countDown();
                try {
                    stopThreads(reader);
                } finally {
                    fixture.flow.close();
                }
            }
        }
    }

    @Test
    public void queuedTimerDoesNotCrossClosingBarrier() throws Exception {
        LifecycleFixture fixture = new LifecycleFixture();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread timer = thread("timer-behind-close", failure, () -> fixture.timerTick.get().run());
        try {
            fixture.releaseInjection.countDown();
            synchronized (fixture.collector.getCheckpointLock()) {
                timer.start();
                awaitThreadState(timer, Thread.State.BLOCKED, failure);
                fixture.flow.triggerBarrier(
                        new CheckpointBarrier(12L, 0L, CheckpointType.COMPLETED_POINT_TYPE));
            }
            timer.join(JOIN_TIMEOUT_MS);
            Assertions.assertFalse(timer.isAlive());
            Assertions.assertNull(failure.get());
            Assertions.assertEquals(1, fixture.records.size());
            Assertions.assertTrue(fixture.records.get(0).getData() instanceof Barrier);
            Assertions.assertEquals(
                    0L, fixture.metrics.counter(MetricNames.FLUSH_SIGNAL_TOTAL).getCount());
        } finally {
            try {
                stopThreads(timer);
            } finally {
                fixture.flow.close();
            }
        }
    }

    private static void assertLifecycleHandoff(boolean timer) throws Exception {
        LifecycleFixture fixture = new LifecycleFixture();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader = thread("lifecycle-reader", failure, fixture.flow::collect);
        Thread injector =
                thread(
                        "lifecycle-injector",
                        failure,
                        () -> {
                            if (timer) {
                                fixture.timerTick.get().run();
                            } else {
                                fixture.flow.triggerBarrier(fixture.barrier);
                            }
                        });
        try {
            reader.start();
            Assertions.assertTrue(
                    fixture.pollEntered.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS));
            injector.start();
            awaitThreadState(injector, Thread.State.BLOCKED, failure);
            fixture.releasePoll.countDown();
            Assertions.assertTrue(
                    fixture.injectionEntered.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS));
            Assertions.assertTrue(
                    fixture.pollReturned.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS));
            // The real reader poll has returned. The real collector is forwarding the control
            // record on another thread, so collect must now wait on the handoff outside its lock.
            awaitThreadState(reader, Thread.State.TIMED_WAITING, failure);
            fixture.releaseInjection.countDown();
            injector.join(JOIN_TIMEOUT_MS);
            reader.join(JOIN_TIMEOUT_MS);
            Assertions.assertFalse(injector.isAlive());
            Assertions.assertFalse(reader.isAlive());
            Assertions.assertNull(failure.get());
            fixture.flow.collect();
            Assertions.assertEquals(3, fixture.records.size());
            Assertions.assertTrue(fixture.records.get(0).getData() instanceof SeaTunnelRow);
            Assertions.assertTrue(fixture.records.get(2).getData() instanceof SeaTunnelRow);
            Assertions.assertTrue(fixture.records.get(1).getData() instanceof FlushSignal == timer);
            if (!timer) {
                Assertions.assertSame(fixture.barrier, fixture.records.get(1).getData());
                Assertions.assertEquals(
                        Arrays.asList("snapshot", "state", "ack", "forward"), fixture.barrierOrder);
                Assertions.assertEquals(1, fixture.savedStates.get().size());
                Assertions.assertArrayEquals(
                        new byte[] {1, 2, 3}, fixture.savedStates.get().get(0));
            }
            Assertions.assertTrue(
                    fixture.metrics.counter(MetricNames.SOURCE_IDLE_NANOS + "#1").getCount() > 0);
        } finally {
            fixture.releasePoll.countDown();
            fixture.releaseInjection.countDown();
            try {
                stopThreads(reader, injector);
            } finally {
                fixture.flow.close();
            }
        }
    }

    private static void awaitThreadState(
            Thread thread, Thread.State expected, AtomicReference<Throwable> failure) {
        Awaitility.await()
                .atMost(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(failure.get());
                            Assertions.assertEquals(expected, thread.getState());
                        });
    }

    /**
     * Captures background failures on the asserting thread and preserves cancellation interrupts.
     *
     * <p>Every caller owns unconditional bounded shutdown through {@link #stopThreads(Thread...)}.
     */
    private static Thread thread(
            String name, AtomicReference<Throwable> failure, CheckedRunnable action) {
        return new Thread(
                () -> {
                    try {
                        action.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                },
                name);
    }

    private static void stopThreads(Thread... threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (Thread thread : threads) {
            thread.join(JOIN_TIMEOUT_MS);
        }
        for (Thread thread : threads) {
            Assertions.assertFalse(
                    thread.isAlive(), "test-owned thread did not terminate: " + thread.getName());
        }
    }

    /**
     * A test action whose checked failures must be observed by the owning assertion thread.
     *
     * <p>This is only thread ownership plumbing; source/collector lifecycle behavior runs normally.
     */
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    /**
     * Drives public lifecycle methods with a real collector and controlled reader/output edges.
     *
     * <p>No private lifecycle method is reflected or replaced: hook registers the timer callback,
     * init creates the reader normally, and actual checkpoint serialization/ack/forwarding runs.
     */
    private static final class LifecycleFixture {
        private final CountDownLatch pollEntered = new CountDownLatch(1);
        private final CountDownLatch releasePoll = new CountDownLatch(1);
        private final CountDownLatch pollReturned = new CountDownLatch(1);
        private final CountDownLatch injectionEntered = new CountDownLatch(1);
        private final CountDownLatch releaseInjection = new CountDownLatch(1);
        private final AtomicBoolean failForward = new AtomicBoolean();
        private final List<Record<?>> records = new ArrayList<>();
        private final List<String> barrierOrder = new ArrayList<>();
        private final AtomicReference<List<byte[]>> savedStates = new AtomicReference<>();
        private final AtomicReference<Runnable> timerTick = new AtomicReference<>();
        private final SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        private final SourceReader<SeaTunnelRow, SourceSplit> reader;
        private final SeaTunnelSourceCollector<SeaTunnelRow> collector;
        private final SourceFlowLifeCycle<SeaTunnelRow, SourceSplit> flow;
        private final CheckpointBarrier barrier =
                new CheckpointBarrier(11L, 0L, CheckpointType.CHECKPOINT_TYPE);

        @SuppressWarnings("unchecked")
        private LifecycleFixture() throws Exception {
            TaskLocation location = new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0);
            SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
            TaskExecutionContext execution = Mockito.mock(TaskExecutionContext.class);
            TaskExecutionService service = Mockito.mock(TaskExecutionService.class);
            Mockito.when(task.getTaskLocation()).thenReturn(location);
            Mockito.when(task.getTaskID()).thenReturn(location.getTaskID());
            Mockito.when(task.getExecutionContext()).thenReturn(execution);
            Mockito.when(task.isObservabilityEnabled()).thenReturn(true);
            Mockito.when(execution.getTaskExecutionService()).thenReturn(service);
            InvocationFuture<Object> address = Mockito.mock(InvocationFuture.class);
            Mockito.when(address.get()).thenReturn(new Address("localhost", 5801));
            Mockito.doReturn(address).when(execution).sendToMaster(Mockito.any());
            Mockito.doAnswer(
                            call -> {
                                timerTick.set(call.getArgument(1));
                                return Mockito.mock(ScheduledFuture.class);
                            })
                    .when(service)
                    .registerTimerFlushTask(Mockito.eq(location), Mockito.any(), Mockito.eq(1000L));
            reader = Mockito.mock(SourceReader.class);
            SeaTunnelSource<SeaTunnelRow, SourceSplit, ?> source =
                    Mockito.mock(SeaTunnelSource.class);
            Mockito.doReturn(reader).when(source).createReader(Mockito.any());
            Mockito.when(source.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
            Serializer<SourceSplit> serializer = Mockito.mock(Serializer.class);
            SourceSplit split = Mockito.mock(SourceSplit.class);
            Mockito.when(serializer.serialize(split)).thenReturn(new byte[] {1, 2, 3});
            Mockito.when(source.getSplitSerializer()).thenReturn(serializer);
            Mockito.when(reader.snapshotState(11L))
                    .thenAnswer(
                            call -> {
                                barrierOrder.add("snapshot");
                                return Collections.singletonList(split);
                            });
            Mockito.doAnswer(
                            call -> {
                                barrierOrder.add("state");
                                savedStates.set(call.getArgument(2));
                                return null;
                            })
                    .when(task)
                    .addState(Mockito.any(), Mockito.any(), Mockito.anyList());
            Mockito.doAnswer(
                            call -> {
                                barrierOrder.add("ack");
                                return null;
                            })
                    .when(task)
                    .ack(Mockito.any());
            OneInputFlowLifeCycle<Record<?>> output =
                    record -> {
                        if (!(record.getData() instanceof SeaTunnelRow)) {
                            if (failForward.get()) {
                                throw new IOException("control forwarding failure");
                            }
                            injectionEntered.countDown();
                            try {
                                Assertions.assertTrue(
                                        releaseInjection.await(
                                                JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException(e);
                            }
                            if (record.getData() instanceof Barrier) {
                                barrierOrder.add("forward");
                            }
                        }
                        records.add(record);
                    };
            collector =
                    new SeaTunnelSourceCollector<>(
                            new Object(),
                            Collections.singletonList(output),
                            metrics,
                            FlowControlStrategy.builder().build(),
                            new SeaTunnelRowType(
                                    new String[] {"id"},
                                    new SeaTunnelDataType[] {BasicType.INT_TYPE}),
                            Collections.emptyList(),
                            task,
                            new EngineConfig(),
                            Collections.emptyMap());
            Mockito.doAnswer(
                            call -> {
                                Collector<SeaTunnelRow> target = call.getArgument(0);
                                synchronized (target.getCheckpointLock()) {
                                    target.collect(new SeaTunnelRow(new Object[] {1}));
                                    pollEntered.countDown();
                                    Assertions.assertTrue(
                                            releasePoll.await(
                                                    JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS));
                                }
                                pollReturned.countDown();
                                return null;
                            })
                    .when(reader)
                    .pollNext(Mockito.any());
            SourceAction<SeaTunnelRow, SourceSplit, ?> action =
                    new SourceAction<>(
                            1L, "source", source, Collections.emptySet(), Collections.emptySet());
            flow =
                    new SourceFlowLifeCycle<>(
                            action,
                            0,
                            location,
                            task,
                            location,
                            new CompletableFuture<>(),
                            metrics,
                            1000L);
            flow.setCollector(collector);
            flow.init();
            flow.hook();
        }
    }
}
