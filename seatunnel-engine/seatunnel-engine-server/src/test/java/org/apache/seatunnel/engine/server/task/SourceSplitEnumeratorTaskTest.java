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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.context.SeaTunnelSplitEnumeratorContext;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.BarrierFlowOperation;
import org.apache.seatunnel.engine.server.task.operation.source.AssignSplitOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.awaitility.Awaitility.await;

public class SourceSplitEnumeratorTaskTest {

    private static final class DummySplit implements SourceSplit {
        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "dummy";
        }
    }

    @Test
    void testOpenShouldBeforeReaderRegister() throws Exception {

        SeaTunnelSource source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator enumerator = Mockito.mock(SourceSplitEnumerator.class);
        Mockito.when(source.createEnumerator(Mockito.any())).thenReturn(enumerator);

        AtomicLong openTime = new AtomicLong(0);
        Mockito.doAnswer(
                        answer -> {
                            openTime.set(System.currentTimeMillis());
                            return null;
                        })
                .when(enumerator)
                .open();

        AtomicLong registerReaderTime = new AtomicLong(0);
        Mockito.doAnswer(
                        answer -> {
                            registerReaderTime.set(System.currentTimeMillis());
                            return null;
                        })
                .when(enumerator)
                .registerReader(Mockito.anyInt());

        SourceAction action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(future.join()).thenReturn(null);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);

        // re-order the method call to test the open() should be called before receivedReader()
        CompletableFuture.runAsync(
                () -> {
                    try {
                        Thread.sleep(1000);
                        enumeratorTask.receivedReader(
                                new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1),
                                Address.createUnresolvedAddress("localhost", 5701));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        while (openTime.get() == 0 || registerReaderTime.get() == 0) {
            enumeratorTask.call();
        }

        Assertions.assertTrue(openTime.get() < registerReaderTime.get());
    }

    @Test
    void testResignalNoMoreSplitsAfterReaderReregister() throws Exception {
        SeaTunnelSource source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator enumerator = Mockito.mock(SourceSplitEnumerator.class);

        AtomicReference<SeaTunnelSplitEnumeratorContext> enumeratorContextRef =
                new AtomicReference<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(
                                    (SeaTunnelSplitEnumeratorContext) invocation.getArgument(0));
                            return enumerator;
                        });

        SourceAction action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(context.sendToMember(Mockito.any(), Mockito.any())).thenReturn(future);
        mockSuccessfulDelivery(future);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        Address address = Address.createUnresolvedAddress("localhost", 5701);

        // Initial register
        enumeratorTask.receivedReader(readerLocation, address);

        SeaTunnelSplitEnumeratorContext enumeratorContext = enumeratorContextRef.get();
        Assertions.assertNotNull(enumeratorContext);

        Mockito.clearInvocations(context);

        // Simulate that NoMoreSplitsEvent has been signaled once.
        enumeratorContext.signalNoMoreSplits(readerLocation.getTaskIndex());
        Assertions.assertTrue(
                enumeratorContext.hasNoMoreSplitsSignaled(readerLocation.getTaskIndex()));

        // Reader re-registers after failover, framework should re-signal.
        enumeratorTask.receivedReader(readerLocation, address);

        Mockito.verify(context, Mockito.times(2)).sendToMember(Mockito.any(), Mockito.any());
    }

    @Test
    void testReceivedReaderUsesEnumeratorContextLock() throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        LockAwareEnumerator enumerator = new LockAwareEnumerator();

        AtomicReference<SeaTunnelSplitEnumeratorContext<DummySplit>> enumeratorContextRef =
                new AtomicReference<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(invocation.getArgument(0));
                            return enumerator;
                        });

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(context.sendToMember(Mockito.any(), Mockito.any())).thenReturn(future);
        mockSuccessfulDelivery(future);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        Address address = Address.createUnresolvedAddress("localhost", 5701);

        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext = enumeratorContextRef.get();
        Assertions.assertNotNull(enumeratorContext);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            synchronized (enumeratorContext) {
                Future<?> blockedRegistration =
                        executorService.submit(
                                () -> {
                                    try {
                                        enumeratorTask.receivedReader(readerLocation, address);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                Assertions.assertFalse(
                        enumerator.registerReaderCalled.await(200, TimeUnit.MILLISECONDS));
                blockedRegistration.cancel(true);
            }

            Future<?> registrationAfterUnlock =
                    executorService.submit(
                            () -> {
                                try {
                                    enumeratorTask.receivedReader(readerLocation, address);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Assertions.assertTrue(enumerator.registerReaderCalled.await(1, TimeUnit.SECONDS));
            registrationAfterUnlock.get(1, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testEnumeratorStateCallbacksUseEnumeratorContextLock() throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        LockAwareEnumerator enumerator = new LockAwareEnumerator();

        AtomicReference<SeaTunnelSplitEnumeratorContext<DummySplit>> enumeratorContextRef =
                new AtomicReference<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(invocation.getArgument(0));
                            return enumerator;
                        });

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(context.sendToMember(Mockito.any(), Mockito.any())).thenReturn(future);
        Mockito.when(future.join()).thenReturn(null);
        mockSuccessfulDelivery(future);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        Address address = Address.createUnresolvedAddress("localhost", 5701);
        enumeratorTask.receivedReader(readerLocation, address);
        enumeratorTask.startCall();

        enumeratorTask.call();
        enumeratorTask.call();
        enumeratorTask.call();

        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext = enumeratorContextRef.get();
        Assertions.assertNotNull(enumeratorContext);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            assertEnumeratorCallWaitsForContextLock(
                    enumeratorContext,
                    executorService,
                    enumerator.splitRequestCalled,
                    () -> enumeratorTask.requestSplit(readerLocation.getTaskIndex()));
            assertEnumeratorCallWaitsForContextLock(
                    enumeratorContext,
                    executorService,
                    enumerator.sourceEventCalled,
                    () -> enumeratorTask.handleSourceEvent(readerLocation.getTaskIndex(), null));
            assertEnumeratorCallWaitsForContextLock(
                    enumeratorContext,
                    executorService,
                    enumerator.addSplitsBackCalled,
                    () ->
                            enumeratorTask.addSplitsBack(
                                    Collections.singletonList(new DummySplit()),
                                    readerLocation.getTaskIndex()));
            assertEnumeratorCallWaitsForContextLock(
                    enumeratorContext,
                    executorService,
                    enumerator.checkpointCompleteCalled,
                    () -> enumeratorTask.notifyCheckpointComplete(1L));
            assertEnumeratorCallWaitsForContextLock(
                    enumeratorContext,
                    executorService,
                    enumerator.checkpointAbortedCalled,
                    () -> enumeratorTask.notifyCheckpointAborted(1L));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testLongRunningEnumeratorRunDoesNotBlockCheckpoint() throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        LongRunningEnumerator enumerator = new LongRunningEnumerator();
        Serializer<Serializable> stateSerializer = Mockito.mock(Serializer.class);
        Mockito.when(source.createEnumerator(Mockito.any())).thenReturn(enumerator);
        Mockito.when(source.getEnumeratorStateSerializer()).thenReturn(stateSerializer);
        Mockito.when(stateSerializer.serialize("state")).thenReturn(new byte[] {1});

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(context.sendToMember(Mockito.any(), Mockito.any())).thenReturn(future);
        Mockito.when(future.join()).thenReturn(null);
        mockSuccessfulDelivery(future);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        Address address = Address.createUnresolvedAddress("localhost", 5701);
        enumeratorTask.receivedReader(readerLocation, address);
        enumeratorTask.startCall();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<?> runningEnumerator =
                    executorService.submit(
                            () -> {
                                enumeratorTask.call();
                                enumeratorTask.call();
                                enumeratorTask.call();
                                enumeratorTask.call();
                                return null;
                            });
            Assertions.assertTrue(enumerator.runStarted.await(1, TimeUnit.SECONDS));

            Assertions.assertTimeoutPreemptively(
                    java.time.Duration.ofSeconds(1),
                    () ->
                            enumeratorTask.triggerBarrier(
                                    new CheckpointBarrier(
                                            1,
                                            System.currentTimeMillis(),
                                            CheckpointType.CHECKPOINT_TYPE)));
            Assertions.assertTrue(enumerator.snapshotCalled.await(1, TimeUnit.SECONDS));

            enumerator.finishRun.countDown();
            runningEnumerator.get(1, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testSplitDeliveryDoesNotWaitForEnumeratorContextMonitor() throws Exception {
        SourceSplitEnumeratorTask<DummySplit> task = Mockito.mock(SourceSplitEnumeratorTask.class);
        TaskExecutionContext executionContext = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> future = Mockito.mock(InvocationFuture.class);
        Mockito.when(task.getExecutionContext()).thenReturn(executionContext);
        Mockito.when(task.getTaskMemberLocationByIndex(0))
                .thenReturn(new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 0));
        Mockito.when(task.getTaskMemberAddressByIndex(0))
                .thenReturn(Address.createUnresolvedAddress("localhost", 5701));
        Mockito.when(executionContext.sendToMember(Mockito.any(), Mockito.any()))
                .thenReturn(future);
        mockSuccessfulDelivery(future);

        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext =
                new SeaTunnelSplitEnumeratorContext<>(1, task, null, null);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            synchronized (enumeratorContext) {
                Future<?> splitDelivery =
                        executorService.submit(() -> enumeratorContext.signalNoMoreSplits(0));
                splitDelivery.get(1, TimeUnit.SECONDS);
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testEarlyEnumeratorOperationWaitsForInitBeforeLockingContext() throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        LockAwareEnumerator enumerator = new LockAwareEnumerator();
        Mockito.when(source.createEnumerator(Mockito.any())).thenReturn(enumerator);

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);
        enumeratorTask.setTaskExecutionContext(context);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<?> earlyRequest =
                    executorService.submit(
                            () -> {
                                enumeratorTask.requestSplit(1L);
                                return null;
                            });
            Assertions.assertFalse(enumerator.splitRequestCalled.await(200, TimeUnit.MILLISECONDS));

            enumeratorTask.init();
            enumeratorTask.restoreState(new ArrayList<>());

            Assertions.assertTrue(enumerator.splitRequestCalled.await(1, TimeUnit.SECONDS));
            earlyRequest.get(1, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testResignalNoMoreSplitsDoesNotBlockReaderRegistrationOnSplitDeliveryAck()
            throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator<DummySplit, Serializable> enumerator =
                Mockito.mock(SourceSplitEnumerator.class);

        AtomicReference<SeaTunnelSplitEnumeratorContext<DummySplit>> enumeratorContextRef =
                new AtomicReference<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(invocation.getArgument(0));
                            return enumerator;
                        });

        CountDownLatch secondReaderRegistered = new CountDownLatch(1);

        Mockito.doAnswer(
                        invocation -> {
                            if (invocation.getArgument(0, Integer.class) == 2) {
                                secondReaderRegistered.countDown();
                            }
                            return null;
                        })
                .when(enumerator)
                .registerReader(Mockito.anyInt());

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(context.sendToMember(Mockito.any(), Mockito.any())).thenReturn(future);
        mockPendingDelivery(future);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext = enumeratorContextRef.get();
        Assertions.assertNotNull(enumeratorContext);
        getNoMoreSplitsSignaledReaders(enumeratorContext).add(1);

        TaskLocation firstReader = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        TaskLocation secondReader = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 2);
        Address firstAddress = Address.createUnresolvedAddress("localhost", 5701);
        Address secondAddress = Address.createUnresolvedAddress("localhost", 5702);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstRegistration =
                    executorService.submit(
                            () -> {
                                try {
                                    enumeratorTask.receivedReader(firstReader, firstAddress);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            firstRegistration.get(1, TimeUnit.SECONDS);

            Future<?> concurrentRegistration =
                    executorService.submit(
                            () -> {
                                try {
                                    enumeratorTask.receivedReader(secondReader, secondAddress);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Assertions.assertTrue(secondReaderRegistered.await(1, TimeUnit.SECONDS));
            concurrentRegistration.get(1, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }

        ArgumentCaptor<Operation> operationCaptor = ArgumentCaptor.forClass(Operation.class);
        Mockito.verify(context).sendToMember(operationCaptor.capture(), Mockito.eq(firstAddress));
        Operation operation = operationCaptor.getValue();
        Assertions.assertInstanceOf(AssignSplitOperation.class, operation);
        Assertions.assertEquals(firstReader, readField(operation, "taskID"));
        Assertions.assertTrue(((java.util.List<?>) readField(operation, "splits")).isEmpty());
    }

    @Test
    void testSignalNoMoreSplitsReturnsBeforeSplitDeliveryCompletes() throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator<DummySplit, Serializable> enumerator =
                Mockito.mock(SourceSplitEnumerator.class);

        AtomicReference<SeaTunnelSplitEnumeratorContext<DummySplit>> enumeratorContextRef =
                new AtomicReference<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(invocation.getArgument(0));
                            return enumerator;
                        });

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(context.sendToMember(Mockito.any(), Mockito.any())).thenReturn(future);
        mockPendingDelivery(future);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        Address address = Address.createUnresolvedAddress("localhost", 5701);
        enumeratorTask.receivedReader(readerLocation, address);

        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext = enumeratorContextRef.get();
        Assertions.assertNotNull(enumeratorContext);

        Assertions.assertTimeoutPreemptively(
                java.time.Duration.ofSeconds(1),
                () -> enumeratorContext.signalNoMoreSplits(readerLocation.getTaskIndex()));
        Assertions.assertTrue(
                enumeratorContext.hasNoMoreSplitsSignaled(readerLocation.getTaskIndex()));
    }

    @Test
    void testThrowIfSplitDeliveryFailedPropagatesAsyncFailure() throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator<DummySplit, Serializable> enumerator =
                Mockito.mock(SourceSplitEnumerator.class);

        AtomicReference<SeaTunnelSplitEnumeratorContext<DummySplit>> enumeratorContextRef =
                new AtomicReference<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(invocation.getArgument(0));
                            return enumerator;
                        });

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(context.sendToMember(Mockito.any(), Mockito.any())).thenReturn(future);
        mockFailedDelivery(future, new RuntimeException("simulated split delivery failure"));
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        Address address = Address.createUnresolvedAddress("localhost", 5701);
        enumeratorTask.receivedReader(readerLocation, address);

        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext = enumeratorContextRef.get();
        Assertions.assertNotNull(enumeratorContext);

        enumeratorContext.signalNoMoreSplits(readerLocation.getTaskIndex());
        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class, enumeratorContext::throwIfSplitDeliveryFailed);
        Assertions.assertEquals(
                "simulated split delivery failure", exception.getCause().getMessage());
    }

    /**
     * Verifies that a synchronous remote-send failure is retained for checkpoint failure
     * propagation.
     */
    @Test
    void testThrowIfSplitDeliveryFailedPropagatesSynchronousSendFailure() {
        SourceSplitEnumeratorTask<DummySplit> task = Mockito.mock(SourceSplitEnumeratorTask.class);
        TaskExecutionContext executionContext = Mockito.mock(TaskExecutionContext.class);
        RuntimeException sendFailure = new RuntimeException("synchronous send failure");
        Mockito.when(task.getExecutionContext()).thenReturn(executionContext);
        Mockito.when(task.getTaskMemberLocationByIndex(0))
                .thenReturn(new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 0));
        Mockito.when(task.getTaskMemberAddressByIndex(0))
                .thenReturn(Address.createUnresolvedAddress("localhost", 5701));
        Mockito.when(executionContext.sendToMember(Mockito.any(), Mockito.any()))
                .thenThrow(sendFailure);

        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext =
                new SeaTunnelSplitEnumeratorContext<>(1, task, null, null);
        enumeratorContext.signalNoMoreSplits(0);

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class, enumeratorContext::throwIfSplitDeliveryFailed);
        Assertions.assertSame(sendFailure, exception.getCause());
    }

    /**
     * Verifies that a split delivery can be enqueued while a checkpoint is waiting for previous
     * acknowledgements without waiting for the enumerator-context monitor.
     */
    @Test
    void testSplitDeliveryReturnsWhileCheckpointWaitsForPendingAcknowledgement() throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator<DummySplit, Serializable> enumerator =
                Mockito.mock(SourceSplitEnumerator.class);
        Serializer<Serializable> stateSerializer = Mockito.mock(Serializer.class);
        AtomicReference<SeaTunnelSplitEnumeratorContext<DummySplit>> enumeratorContextRef =
                new AtomicReference<>();
        List<String> sequence = new CopyOnWriteArrayList<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(invocation.getArgument(0));
                            return enumerator;
                        });
        Mockito.when(source.getEnumeratorStateSerializer()).thenReturn(stateSerializer);
        Mockito.when(enumerator.snapshotState(1L))
                .thenAnswer(
                        invocation -> {
                            sequence.add("snapshot");
                            return "state";
                        });
        Mockito.when(stateSerializer.serialize("state")).thenReturn(new byte[] {1});

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);
        TaskExecutionContext executionContext = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> firstDelivery = Mockito.mock(InvocationFuture.class);
        InvocationFuture<Object> successfulDelivery = Mockito.mock(InvocationFuture.class);
        AtomicReference<BiConsumer<Object, Throwable>> firstDeliveryCallback =
                new AtomicReference<>();
        AtomicInteger assignSplitSendCount = new AtomicInteger();
        Mockito.when(executionContext.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(executionContext.sendToMaster(Mockito.any())).thenReturn(successfulDelivery);
        Mockito.when(successfulDelivery.join()).thenReturn(null);
        mockSuccessfulDelivery(successfulDelivery);
        Mockito.doAnswer(
                        invocation -> {
                            firstDeliveryCallback.set(invocation.getArgument(0));
                            return firstDelivery;
                        })
                .when(firstDelivery)
                .whenComplete(Mockito.any());
        Mockito.when(executionContext.sendToMember(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            Operation operation = invocation.getArgument(0);
                            if (operation instanceof AssignSplitOperation) {
                                int sendNumber = assignSplitSendCount.incrementAndGet();
                                if (sendNumber == 1) {
                                    return firstDelivery;
                                }
                                sequence.add("second-send");
                            }
                            return successfulDelivery;
                        });
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(executionContext.getTaskExecutionService()).thenReturn(taskExecutionService);
        enumeratorTask.setTaskExecutionContext(executionContext);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        enumeratorTask.receivedReader(
                readerLocation, Address.createUnresolvedAddress("localhost", 5701));
        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext = enumeratorContextRef.get();
        enumeratorContext.signalNoMoreSplits(readerLocation.getTaskIndex());

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            AtomicReference<Thread> checkpointThread = new AtomicReference<>();
            Future<?> checkpoint =
                    executorService.submit(
                            () -> {
                                checkpointThread.set(Thread.currentThread());
                                enumeratorTask.triggerBarrier(
                                        new CheckpointBarrier(
                                                1,
                                                System.currentTimeMillis(),
                                                CheckpointType.CHECKPOINT_TYPE));
                                return null;
                            });
            await().atMost(5, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    checkpointThread.get() != null
                                            && checkpointThread.get().getState()
                                                    == Thread.State.WAITING);

            CountDownLatch secondDeliveryStarted = new CountDownLatch(1);
            Future<?> secondDelivery =
                    executorService.submit(
                            () -> {
                                secondDeliveryStarted.countDown();
                                enumeratorContext.signalNoMoreSplits(readerLocation.getTaskIndex());
                            });
            Assertions.assertTrue(secondDeliveryStarted.await(5, TimeUnit.SECONDS));
            secondDelivery.get(1, TimeUnit.SECONDS);
            Assertions.assertEquals(1, assignSplitSendCount.get());

            firstDeliveryCallback.get().accept(null, null);
            checkpoint.get(5, TimeUnit.SECONDS);
            await().atMost(5, TimeUnit.SECONDS).until(() -> assignSplitSendCount.get() == 2);

            Assertions.assertEquals(2, assignSplitSendCount.get());
            Assertions.assertTrue(sequence.contains("snapshot"));
            Assertions.assertTrue(sequence.contains("second-send"));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testCheckpointWaitsForDeliveryEnqueuedDuringSnapshotBeforeReaderBarrier()
            throws Exception {
        SeaTunnelSource<?, DummySplit, Serializable> source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator<DummySplit, Serializable> enumerator =
                Mockito.mock(SourceSplitEnumerator.class);
        Serializer<Serializable> stateSerializer = Mockito.mock(Serializer.class);
        AtomicReference<SeaTunnelSplitEnumeratorContext<DummySplit>> enumeratorContextRef =
                new AtomicReference<>();
        CountDownLatch snapshotStarted = new CountDownLatch(1);
        CountDownLatch deliverySendStarted = new CountDownLatch(1);
        AtomicReference<BiConsumer<Object, Throwable>> deliveryCallback = new AtomicReference<>();
        AtomicInteger barrierSendCount = new AtomicInteger();
        List<String> sequence = new CopyOnWriteArrayList<>();
        Mockito.when(source.createEnumerator(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            enumeratorContextRef.set(invocation.getArgument(0));
                            return enumerator;
                        });
        Mockito.when(source.getEnumeratorStateSerializer()).thenReturn(stateSerializer);
        Mockito.when(enumerator.snapshotState(1L))
                .thenAnswer(
                        invocation -> {
                            snapshotStarted.countDown();
                            Assertions.assertTrue(deliverySendStarted.await(5, TimeUnit.SECONDS));
                            sequence.add("snapshot");
                            return "state";
                        });
        Mockito.when(stateSerializer.serialize("state")).thenReturn(new byte[] {1});

        SourceAction<?, DummySplit, Serializable> action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask<DummySplit> enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);
        TaskExecutionContext executionContext = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture<Object> pendingDelivery = Mockito.mock(InvocationFuture.class);
        InvocationFuture<Object> successfulDelivery = Mockito.mock(InvocationFuture.class);
        Mockito.when(executionContext.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(executionContext.sendToMaster(Mockito.any())).thenReturn(successfulDelivery);
        Mockito.when(successfulDelivery.join()).thenReturn(null);
        mockSuccessfulDelivery(successfulDelivery);
        Mockito.doAnswer(
                        invocation -> {
                            deliveryCallback.set(invocation.getArgument(0));
                            return pendingDelivery;
                        })
                .when(pendingDelivery)
                .whenComplete(Mockito.any());
        Mockito.when(executionContext.sendToMember(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            Operation operation = invocation.getArgument(0);
                            if (operation instanceof AssignSplitOperation) {
                                deliverySendStarted.countDown();
                                return pendingDelivery;
                            }
                            if (operation instanceof BarrierFlowOperation) {
                                sequence.add("barrier");
                                barrierSendCount.incrementAndGet();
                            }
                            return successfulDelivery;
                        });
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(executionContext.getTaskExecutionService()).thenReturn(taskExecutionService);
        enumeratorTask.setTaskExecutionContext(executionContext);
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        TaskLocation readerLocation = new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1);
        enumeratorTask.receivedReader(
                readerLocation, Address.createUnresolvedAddress("localhost", 5701));
        SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext = enumeratorContextRef.get();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Future<?> checkpoint =
                    executorService.submit(
                            () -> {
                                enumeratorTask.triggerBarrier(
                                        new CheckpointBarrier(
                                                1,
                                                System.currentTimeMillis(),
                                                CheckpointType.CHECKPOINT_TYPE));
                                return null;
                            });
            Assertions.assertTrue(snapshotStarted.await(5, TimeUnit.SECONDS));

            Future<?> splitDelivery =
                    executorService.submit(
                            () -> {
                                enumeratorContext.signalNoMoreSplits(readerLocation.getTaskIndex());
                                return null;
                            });
            splitDelivery.get(5, TimeUnit.SECONDS);
            await().atMost(5, TimeUnit.SECONDS).until(() -> deliveryCallback.get() != null);

            Assertions.assertFalse(checkpoint.isDone());
            Assertions.assertEquals(0, barrierSendCount.get());

            sequence.add("delivery-ack");
            deliveryCallback.get().accept(null, null);
            checkpoint.get(5, TimeUnit.SECONDS);

            Assertions.assertEquals(1, barrierSendCount.get());
            Assertions.assertTrue(sequence.indexOf("delivery-ack") < sequence.indexOf("barrier"));
        } finally {
            executorService.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private Set<Integer> getNoMoreSplitsSignaledReaders(
            SeaTunnelSplitEnumeratorContext<DummySplit> enumeratorContext) throws Exception {
        return (Set<Integer>) readField(enumeratorContext, "noMoreSplitsSignaledReaders");
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void assertEnumeratorCallWaitsForContextLock(
            Object enumeratorContext,
            ExecutorService executorService,
            CountDownLatch callbackCalled,
            CheckedRunnable callback)
            throws Exception {
        Future<?> blockedCall;
        synchronized (enumeratorContext) {
            blockedCall =
                    executorService.submit(
                            () -> {
                                callback.run();
                                return null;
                            });
            Assertions.assertFalse(callbackCalled.await(200, TimeUnit.MILLISECONDS));
        }

        Assertions.assertTrue(callbackCalled.await(1, TimeUnit.SECONDS));
        blockedCall.get(1, TimeUnit.SECONDS);
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    /**
     * Completes the mocked split delivery immediately to simulate a normal remote acknowledgement.
     */
    @SuppressWarnings("unchecked")
    private static void mockSuccessfulDelivery(InvocationFuture<?> future) {
        Mockito.doAnswer(
                        invocation -> {
                            BiConsumer<Object, Throwable> callback = invocation.getArgument(0);
                            callback.accept(null, null);
                            return future;
                        })
                .when(future)
                .whenComplete(Mockito.any());
    }

    /**
     * Keeps the mocked split delivery pending so tests can assert non-blocking task behavior.
     *
     * <p>The returned future is completed explicitly by the test after the enumerator advances.
     */
    private static void mockPendingDelivery(InvocationFuture<?> future) {
        Mockito.doAnswer(invocation -> future).when(future).whenComplete(Mockito.any());
    }

    /**
     * Completes the mocked split delivery exceptionally to verify async failure propagation.
     *
     * <p>The helper exposes the exact transport error to the enumerator failure path.
     */
    @SuppressWarnings("unchecked")
    private static void mockFailedDelivery(InvocationFuture<?> future, Throwable throwable) {
        Mockito.doAnswer(
                        invocation -> {
                            BiConsumer<Object, Throwable> callback = invocation.getArgument(0);
                            callback.accept(null, throwable);
                            return future;
                        })
                .when(future)
                .whenComplete(Mockito.any());
    }

    private static final class LockAwareEnumerator
            implements SourceSplitEnumerator<DummySplit, Serializable> {

        private final CountDownLatch registerReaderCalled = new CountDownLatch(1);
        private final CountDownLatch addSplitsBackCalled = new CountDownLatch(1);
        private final CountDownLatch splitRequestCalled = new CountDownLatch(1);
        private final CountDownLatch sourceEventCalled = new CountDownLatch(1);
        private final CountDownLatch checkpointCompleteCalled = new CountDownLatch(1);
        private final CountDownLatch checkpointAbortedCalled = new CountDownLatch(1);

        @Override
        public void open() {}

        @Override
        public void run() {}

        @Override
        public void close() {}

        @Override
        public void addSplitsBack(java.util.List<DummySplit> splits, int subtaskId) {
            addSplitsBackCalled.countDown();
        }

        @Override
        public int currentUnassignedSplitSize() {
            return 0;
        }

        @Override
        public void handleSplitRequest(int subtaskId) {
            splitRequestCalled.countDown();
        }

        @Override
        public void registerReader(int subtaskId) {
            registerReaderCalled.countDown();
        }

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
            sourceEventCalled.countDown();
        }

        @Override
        public Serializable snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            checkpointCompleteCalled.countDown();
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            checkpointAbortedCalled.countDown();
        }
    }

    private static final class LongRunningEnumerator
            implements SourceSplitEnumerator<DummySplit, Serializable> {

        private final CountDownLatch runStarted = new CountDownLatch(1);
        private final CountDownLatch finishRun = new CountDownLatch(1);
        private final CountDownLatch snapshotCalled = new CountDownLatch(1);

        @Override
        public void open() {}

        @Override
        public void run() throws Exception {
            runStarted.countDown();
            finishRun.await();
        }

        @Override
        public void close() {}

        @Override
        public void addSplitsBack(List<DummySplit> splits, int subtaskId) {}

        @Override
        public int currentUnassignedSplitSize() {
            return 0;
        }

        @Override
        public void handleSplitRequest(int subtaskId) {}

        @Override
        public void registerReader(int subtaskId) {}

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}

        @Override
        public Serializable snapshotState(long checkpointId) {
            snapshotCalled.countDown();
            return "state";
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }
}
