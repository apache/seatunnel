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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.context.SeaTunnelSplitEnumeratorContext;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
        Mockito.when(future.join()).thenReturn(null);
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
        Mockito.when(future.join()).thenReturn(null);
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
    void testResignalNoMoreSplitsOutsideLockWithPreciseAssignSplitVerification() throws Exception {
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

        CountDownLatch blockingJoinEntered = new CountDownLatch(1);
        CountDownLatch allowBlockingJoin = new CountDownLatch(1);
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
        Mockito.when(future.join())
                .thenAnswer(
                        invocation -> {
                            blockingJoinEntered.countDown();
                            allowBlockingJoin.await(5, TimeUnit.SECONDS);
                            return null;
                        });
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
            Future<?> blockingFuture =
                    executorService.submit(
                            () -> {
                                try {
                                    enumeratorTask.receivedReader(firstReader, firstAddress);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Assertions.assertTrue(blockingJoinEntered.await(1, TimeUnit.SECONDS));

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

            allowBlockingJoin.countDown();
            blockingFuture.get(1, TimeUnit.SECONDS);
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

    private static final class LockAwareEnumerator
            implements SourceSplitEnumerator<DummySplit, Serializable> {

        private final CountDownLatch registerReaderCalled = new CountDownLatch(1);

        @Override
        public void open() {}

        @Override
        public void run() {}

        @Override
        public void close() {}

        @Override
        public void addSplitsBack(java.util.List<DummySplit> splits, int subtaskId) {}

        @Override
        public int currentUnassignedSplitSize() {
            return 0;
        }

        @Override
        public void handleSplitRequest(int subtaskId) {}

        @Override
        public void registerReader(int subtaskId) {
            registerReaderCalled.countDown();
        }

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}

        @Override
        public Serializable snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }
}
