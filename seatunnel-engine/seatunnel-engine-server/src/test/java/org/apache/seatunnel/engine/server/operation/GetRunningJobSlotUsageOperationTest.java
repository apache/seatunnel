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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineRetryableException;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.jet.impl.LiveOperationRegistry;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Verifies the asynchronous response lifecycle of {@link GetRunningJobSlotUsageOperation}: the
 * operation thread must be released while the aggregation is still pending, and exactly one success
 * or failure response must be sent once the aggregation completes.
 */
class GetRunningJobSlotUsageOperationTest {

    private final List<Runnable> pendingTasks = new ArrayList<>();
    private final List<Object> responses = new ArrayList<>();

    @Test
    void shouldReleaseOperationThreadBeforeAggregationCompletes() {
        GetRunningJobSlotUsageOperation operation = prepareOperation(service -> "[]");

        // The deferred executor never runs the task on its own, so run() can only return when it
        // does not wait on the aggregation future.
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), operation::run);
        Assertions.assertEquals(1, pendingTasks.size());
        Assertions.assertTrue(responses.isEmpty());

        pendingTasks.get(0).run();

        Assertions.assertEquals(Collections.singletonList("[]"), responses);
    }

    @Test
    void shouldSendBuilderFailureAsExceptionalResponse() {
        SeaTunnelEngineRetryableException failure =
                new SeaTunnelEngineRetryableException("retryable");
        GetRunningJobSlotUsageOperation operation =
                prepareOperation(
                        service -> {
                            throw failure;
                        });

        operation.run();
        pendingTasks.get(0).run();

        // The original exception reaches the caller, not the CompletionException wrapper.
        Assertions.assertEquals(1, responses.size());
        Assertions.assertSame(failure, responses.get(0));
    }

    private GetRunningJobSlotUsageOperation prepareOperation(
            Function<SeaTunnelServer, String> responseBuilder) {
        ManagedExecutorService executor = Mockito.mock(ManagedExecutorService.class);
        Mockito.doAnswer(
                        invocation -> {
                            pendingTasks.add(invocation.getArgument(0));
                            return null;
                        })
                .when(executor)
                .execute(Mockito.any());
        ExecutionService executionService = Mockito.mock(ExecutionService.class);
        Mockito.when(executionService.getExecutor(GetRunningJobSlotUsageOperation.EXECUTOR_NAME))
                .thenReturn(executor);
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        Mockito.when(nodeEngine.getExecutionService()).thenReturn(executionService);
        Mockito.when(nodeEngine.getLogger(Mockito.any(Class.class)))
                .thenReturn(Logger.getLogger(GetRunningJobSlotUsageOperationTest.class));
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        Mockito.when(server.getLiveOperationRegistry())
                .thenReturn(Mockito.mock(LiveOperationRegistry.class));

        GetRunningJobSlotUsageOperation operation =
                new GetRunningJobSlotUsageOperation() {
                    @Override
                    String buildResponse(SeaTunnelServer service) {
                        return responseBuilder.apply(service);
                    }
                };
        operation.setNodeEngine(nodeEngine);
        operation.setService(server);
        operation.setOperationResponseHandler((op, response) -> responses.add(response));
        return operation;
    }
}
