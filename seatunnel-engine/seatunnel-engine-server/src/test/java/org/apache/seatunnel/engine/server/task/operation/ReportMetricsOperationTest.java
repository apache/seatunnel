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

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class ReportMetricsOperationTest {

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
