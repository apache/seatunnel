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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.operation.GetNodeHttpPortOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class ReportMetricsOperationTest {

    @Test
    void shouldKeepRemoteGenericOperationsResponsiveWhileMetricsWaitForOffloadExecutor()
            throws Exception {
        String clusterName =
                TestUtils.getClusterName("ReportMetricsOperationTest_remoteInvocationOffload");
        SeaTunnelConfig masterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        masterConfig.getHazelcastConfig().setClusterName(clusterName);
        masterConfig
                .getHazelcastConfig()
                .setProperty("hazelcast.operation.generic.thread.count", "4");
        HazelcastInstanceImpl master = SeaTunnelServerStarter.createHazelcastInstance(masterConfig);
        HazelcastInstanceImpl caller = SeaTunnelServerStarter.createHazelcastInstance(clusterName);
        CountDownLatch releaseOffloadExecutor = new CountDownLatch(1);
        CountDownLatch releaseGenericOperations = new CountDownLatch(1);

        try {
            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(2, master.getCluster().getMembers().size());
                                Assertions.assertEquals(2, caller.getCluster().getMembers().size());
                            });

            NodeEngineImpl masterEngine = master.node.getNodeEngine();
            NodeEngineImpl callerEngine = caller.node.getNodeEngine();
            Assertions.assertEquals(masterEngine.getThisAddress(), callerEngine.getMasterAddress());
            Assertions.assertNotEquals(
                    callerEngine.getThisAddress(), callerEngine.getMasterAddress());

            ManagedExecutorService offloadExecutor =
                    masterEngine
                            .getExecutionService()
                            .getExecutor(ExecutionService.OFFLOADABLE_EXECUTOR);
            CountDownLatch allOffloadThreadsBlocked =
                    new CountDownLatch(offloadExecutor.getMaximumPoolSize());
            for (int i = 0; i < offloadExecutor.getMaximumPoolSize(); i++) {
                offloadExecutor.execute(
                        () -> {
                            allOffloadThreadsBlocked.countDown();
                            try {
                                releaseOffloadExecutor.await(20, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
            }
            Assertions.assertTrue(allOffloadThreadsBlocked.await(20, TimeUnit.SECONDS));

            CountDownLatch genericOperationsStarted = new CountDownLatch(3);
            for (int i = 0; i < 3; i++) {
                masterEngine
                        .getOperationService()
                        .execute(
                                new BlockingGenericOperation(
                                        genericOperationsStarted, releaseGenericOperations));
            }
            Assertions.assertTrue(genericOperationsStarted.await(20, TimeUnit.SECONDS));

            TaskLocation taskLocation = new TaskLocation();
            taskLocation.setTaskID(1);
            InvocationFuture<Object> metricsInvocation =
                    invoke(
                            callerEngine,
                            new ReportMetricsOperation(
                                    Collections.singletonMap(
                                            taskLocation, new SeaTunnelMetricsContext())));

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(offloadExecutor.getQueueSize() > 0));
            Assertions.assertFalse(metricsInvocation.isDone());

            Object httpPort =
                    callerEngine
                            .getOperationService()
                            .createInvocationBuilder(
                                    SeaTunnelServer.SERVICE_NAME,
                                    new GetNodeHttpPortOperation(),
                                    callerEngine.getMasterAddress())
                            .invoke()
                            .get(10, TimeUnit.SECONDS);
            Assertions.assertInstanceOf(Integer.class, httpPort);

            releaseOffloadExecutor.countDown();
            metricsInvocation.get(10, TimeUnit.SECONDS);

            SeaTunnelServer masterServer = masterEngine.getService(SeaTunnelServer.SERVICE_NAME);
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1,
                                            masterServer
                                                    .getEngineContext()
                                                    .getStateStores()
                                                    .metricsSnapshotStore()
                                                    .size()));
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertEquals(0, offloadExecutor.getQueueSize()));
        } finally {
            releaseOffloadExecutor.countDown();
            releaseGenericOperations.countDown();
            caller.shutdown();
            master.shutdown();
        }
    }

    @Test
    void shouldAttemptSuccessResponseOnlyOnceWhenResponseDeliveryFails() throws Exception {
        AtomicInteger responseAttempts = new AtomicInteger();
        RuntimeException responseFailure = new RuntimeException("response delivery failed");
        ReportMetricsOperation operation =
                new BlockingReportMetricsOperation(null, null, null, null, false);
        operation.setOperationResponseHandler(
                (ignoredOperation, ignoredResponse) -> {
                    responseAttempts.incrementAndGet();
                    throw responseFailure;
                });
        ExecutionService directExecutionService =
                (ExecutionService)
                        Proxy.newProxyInstance(
                                ExecutionService.class.getClassLoader(),
                                new Class<?>[] {ExecutionService.class},
                                (proxy, method, arguments) -> {
                                    Assertions.assertEquals("execute", method.getName());
                                    Assertions.assertEquals(
                                            ExecutionService.OFFLOADABLE_EXECUTOR, arguments[0]);
                                    ((Runnable) arguments[1]).run();
                                    return null;
                                });

        Offload offload = (Offload) operation.call();
        ReflectionUtils.setField(
                offload, Offload.class, "executionService", directExecutionService);

        RuntimeException actual = Assertions.assertThrows(RuntimeException.class, offload::start);
        Assertions.assertSame(responseFailure, actual);
        Assertions.assertEquals(1, responseAttempts.get());
    }

    @Test
    void shouldOffloadExecutionAndCompleteInvocationAfterMetricsUpdate() throws Exception {
        String clusterName = TestUtils.getClusterName("ReportMetricsOperationTest");
        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);

        try {
            NodeEngineImpl nodeEngine = instance.node.getNodeEngine();
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<String> executionThread = new AtomicReference<>();
            long completedTaskCount =
                    nodeEngine
                            .getExecutionService()
                            .getExecutor(ExecutionService.OFFLOADABLE_EXECUTOR)
                            .getCompletedTaskCount();

            InvocationFuture<Object> invocation =
                    invoke(
                            nodeEngine,
                            new BlockingReportMetricsOperation(
                                    started, release, executionThread, null, false));

            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(invocation.isDone());
            Assertions.assertFalse(executionThread.get().contains("generic-operation"));

            release.countDown();
            invocation.get(10, TimeUnit.SECONDS);

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            nodeEngine
                                                            .getExecutionService()
                                                            .getExecutor(
                                                                    ExecutionService
                                                                            .OFFLOADABLE_EXECUTOR)
                                                            .getCompletedTaskCount()
                                                    > completedTaskCount));

            IllegalStateException expected = new IllegalStateException("metrics update failed");
            ExecutionException failure =
                    Assertions.assertThrows(
                            ExecutionException.class,
                            () ->
                                    invoke(
                                                    nodeEngine,
                                                    new BlockingReportMetricsOperation(
                                                            null, null, null, expected, true))
                                            .get(10, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(IllegalStateException.class, failure.getCause());
            Assertions.assertEquals(expected.getMessage(), failure.getCause().getMessage());
        } finally {
            instance.shutdown();
        }
    }

    private InvocationFuture<Object> invoke(
            NodeEngineImpl nodeEngine, ReportMetricsOperation operation) {
        return nodeEngine
                .getOperationService()
                .createInvocationBuilder(
                        SeaTunnelServer.SERVICE_NAME, operation, nodeEngine.getMasterAddress())
                .invoke();
    }

    private static class BlockingGenericOperation extends Operation {
        private final CountDownLatch started;
        private final CountDownLatch release;

        private BlockingGenericOperation(CountDownLatch started, CountDownLatch release) {
            this.started = started;
            this.release = release;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                release.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class BlockingReportMetricsOperation extends ReportMetricsOperation {
        private final CountDownLatch started;
        private final CountDownLatch release;
        private final AtomicReference<String> executionThread;
        private final RuntimeException failure;
        private final boolean failFailureHooks;

        private BlockingReportMetricsOperation(
                CountDownLatch started,
                CountDownLatch release,
                AtomicReference<String> executionThread,
                RuntimeException failure,
                boolean failFailureHooks) {
            this.started = started;
            this.release = release;
            this.executionThread = executionThread;
            this.failure = failure;
            this.failFailureHooks = failFailureHooks;
        }

        @Override
        public void runInternal() throws Exception {
            if (failure != null) {
                throw failure;
            }
            if (started == null) {
                return;
            }
            executionThread.set(Thread.currentThread().getName());
            started.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
        }

        @Override
        public void onExecutionFailure(Throwable failure) {
            if (failFailureHooks) {
                throw new IllegalStateException("failure callback failed");
            }
        }

        @Override
        public void logError(Throwable failure) {
            if (failFailureHooks) {
                throw new IllegalStateException("failure logging failed");
            }
            super.logError(failure);
        }
    }
}
