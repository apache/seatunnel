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

import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.error.ErrorHandler;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerMode;
import org.apache.seatunnel.engine.server.task.error.ErrorSinkRowWriter;
import org.apache.seatunnel.engine.server.task.error.StageErrorConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Verifies the runtime's best-effort transform cleanup contract. */
class TransformFlowLifeCycleCloseTest {

    @Test
    void closeLogsTransformFailureAndContinuesCleanup() throws Exception {
        SeaTunnelTransform<SeaTunnelRow> failingTransform = Mockito.mock(SeaTunnelTransform.class);
        SeaTunnelTransform<SeaTunnelRow> followingTransform =
                Mockito.mock(SeaTunnelTransform.class);
        Mockito.when(failingTransform.getPluginName()).thenReturn("failing");
        Mockito.doThrow(new RuntimeException("close failed")).when(failingTransform).close();

        TransformChainAction<SeaTunnelRow> action =
                new TransformChainAction<>(
                        1L,
                        "close-test",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Arrays.asList(failingTransform, followingTransform));
        Collector<org.apache.seatunnel.api.table.type.Record<?>> collector =
                Mockito.mock(Collector.class);
        TransformFlowLifeCycle<SeaTunnelRow> flow =
                new TransformFlowLifeCycle<>(
                        action,
                        Mockito.mock(SeaTunnelTask.class),
                        collector,
                        new CompletableFuture<>());

        Assertions.assertDoesNotThrow(flow::close);
        Mockito.verify(failingTransform).close();
        Mockito.verify(followingTransform).close();
    }

    static Stream<Arguments> startupFailureCases() {
        return IntStream.range(0, 3)
                .boxed()
                .flatMap(
                        position ->
                                Stream.of(false, true)
                                        .flatMap(
                                                wrapped ->
                                                        Stream.of(false, true)
                                                                .map(
                                                                        flatMap ->
                                                                                Arguments.of(
                                                                                        position,
                                                                                        wrapped,
                                                                                        flatMap))));
    }

    @ParameterizedTest
    @MethodSource("startupFailureCases")
    void failedStartupClosesPartialResourcesAndRejectsRows(
            int failingPosition, boolean wrapped, boolean flatMap) throws Exception {
        StartupChain chain = new StartupChain(failingPosition, wrapped, flatMap, false);

        Assertions.assertSame(
                chain.failure, Assertions.assertThrows(RuntimeException.class, chain.flow::open));
        Assertions.assertEquals(
                0, chain.resources.get(), "Partial startup resources must be released");
        for (int i = 0; i < chain.children.size(); i++) {
            Mockito.verify(chain.children.get(i), Mockito.times(i <= failingPosition ? 1 : 0))
                    .open();
            Mockito.verify(chain.children.get(i)).close();
        }
        IllegalStateException rejected =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> chain.flow.received(new Record<>("row")));
        Assertions.assertSame(chain.failure, rejected.getCause());
        Assertions.assertEquals(0, chain.mapCalls.get());
        Mockito.verifyNoInteractions(chain.collector);
        chain.flow.close();
        for (SeaTunnelTransform<String> child : chain.children) {
            Mockito.verify(child, Mockito.times(1)).close();
        }
        Assertions.assertSame(
                chain.failure, Assertions.assertThrows(RuntimeException.class, chain.flow::open));
    }

    @Test
    void startupFailureSurvivesChildAndErrorHandlerCleanupFailures() throws Exception {
        StartupChain chain = new StartupChain(1, true, false, true);
        ErrorHandler<String> errorHandler = Mockito.mock(ErrorHandler.class);
        RuntimeException closeFailure = new RuntimeException("error handler close failed");
        Mockito.doThrow(closeFailure).when(errorHandler).close();
        Field handlerField = TransformFlowLifeCycle.class.getDeclaredField("errorHandler");
        handlerField.setAccessible(true);
        Mockito.doAnswer(
                        call -> {
                            chain.resources.incrementAndGet();
                            chain.allocated.get(1).set(true);
                            handlerField.set(chain.flow, errorHandler);
                            throw chain.failure;
                        })
                .when(chain.children.get(1))
                .open();

        Assertions.assertSame(
                chain.failure, Assertions.assertThrows(RuntimeException.class, chain.flow::open));
        Assertions.assertEquals(0, chain.resources.get());
        Assertions.assertEquals(1, chain.failure.getSuppressed().length);
        Assertions.assertSame(closeFailure, chain.failure.getSuppressed()[0].getCause());
        for (SeaTunnelTransform<String> child : chain.children) {
            Mockito.verify(child).close();
        }
        chain.flow.close();
        Mockito.verify(errorHandler, Mockito.times(1)).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedStartupClosesRouteWriterOnce(boolean failClose) throws Exception {
        StartupChain chain = new StartupChain(1, true, false, false);
        ErrorSinkRowWriter<String> writer = Mockito.mock(ErrorSinkRowWriter.class);
        if (failClose) {
            Mockito.doThrow(new RuntimeException("route writer close failed")).when(writer).close();
        }
        ErrorHandler<String> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(), writer);
        Field handlerField = TransformFlowLifeCycle.class.getDeclaredField("errorHandler");
        handlerField.setAccessible(true);
        Mockito.doAnswer(
                        call -> {
                            chain.resources.incrementAndGet();
                            chain.allocated.get(1).set(true);
                            handlerField.set(chain.flow, handler);
                            throw chain.failure;
                        })
                .when(chain.children.get(1))
                .open();

        Assertions.assertSame(
                chain.failure, Assertions.assertThrows(RuntimeException.class, chain.flow::open));
        Assertions.assertEquals(0, chain.resources.get());
        Assertions.assertEquals(failClose ? 1 : 0, chain.failure.getSuppressed().length);
        chain.flow.close();
        Mockito.verify(writer, Mockito.times(1)).close();
    }

    @ParameterizedTest
    @MethodSource("successfulStartupCases")
    void successfulStartupRetainsRowAndCleanupBehavior(boolean wrapped, boolean flatMap)
            throws Exception {
        StartupChain chain = new StartupChain(-1, wrapped, flatMap, false);
        chain.flow.open();
        Assertions.assertEquals(3, chain.resources.get());
        Record<String> row = new Record<>("row");
        chain.flow.received(row);
        Assertions.assertEquals(3, chain.mapCalls.get());
        Mockito.verify(chain.collector)
                .collect(Mockito.argThat(record -> "row".equals(record.getData())));
        chain.flow.close();
        Assertions.assertEquals(0, chain.resources.get());
    }

    static Stream<Arguments> successfulStartupCases() {
        return Stream.of(false, true)
                .flatMap(
                        wrapped ->
                                Stream.of(false, true)
                                        .map(flatMap -> Arguments.of(wrapped, flatMap)));
    }

    private static final class StartupChain {
        private final RuntimeException failure =
                new RuntimeException("transform initialization failed");
        private final AtomicInteger resources = new AtomicInteger();
        private final AtomicInteger mapCalls = new AtomicInteger();
        private final List<AtomicBoolean> allocated = new ArrayList<>();
        private final List<SeaTunnelTransform<String>> children = new ArrayList<>();
        private final Collector<Record<?>> collector = Mockito.mock(Collector.class);
        private final TransformFlowLifeCycle<String> flow;

        private StartupChain(
                int failingPosition, boolean wrapped, boolean flatMap, boolean failClose)
                throws Exception {
            for (int i = 0; i < 3; i++) {
                final int position = i;
                AtomicBoolean resource = new AtomicBoolean();
                allocated.add(resource);
                SeaTunnelTransform<String> child;
                if (flatMap) {
                    SeaTunnelFlatMapTransform<String> flat =
                            Mockito.mock(SeaTunnelFlatMapTransform.class);
                    Mockito.when(flat.flatMap(Mockito.any()))
                            .thenAnswer(
                                    call -> {
                                        mapCalls.incrementAndGet();
                                        return Collections.singletonList(call.getArgument(0));
                                    });
                    child = flat;
                } else {
                    SeaTunnelMapTransform<String> map = Mockito.mock(SeaTunnelMapTransform.class);
                    Mockito.when(map.map(Mockito.any()))
                            .thenAnswer(
                                    call -> {
                                        mapCalls.incrementAndGet();
                                        return call.getArgument(0);
                                    });
                    child = map;
                }
                Mockito.when(child.getPluginName()).thenReturn("startup-" + i);
                Mockito.doAnswer(
                                call -> {
                                    resource.set(true);
                                    resources.incrementAndGet();
                                    if (position == failingPosition) {
                                        throw failure;
                                    }
                                    return null;
                                })
                        .when(child)
                        .open();
                Mockito.doAnswer(
                                call -> {
                                    if (resource.getAndSet(false)) {
                                        resources.decrementAndGet();
                                    }
                                    if (failClose && position == 0) {
                                        throw new RuntimeException("child close failed");
                                    }
                                    return null;
                                })
                        .when(child)
                        .close();
                children.add(child);
            }
            SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
            SeaTunnelMetricsContext metrics = Mockito.mock(SeaTunnelMetricsContext.class);
            Mockito.when(metrics.counter(Mockito.anyString()))
                    .thenAnswer(call -> new ThreadSafeCounter(call.getArgument(0)));
            Mockito.when(task.getMetricsContext()).thenReturn(metrics);
            Mockito.when(task.getJobEnvOptions()).thenReturn(Collections.emptyMap());
            Mockito.when(task.getEnvOptions())
                    .thenReturn(
                            wrapped
                                    ? Collections.singletonMap(
                                            "transform_error_handler",
                                            Collections.singletonMap("mode", "LOG"))
                                    : Collections.emptyMap());
            TransformChainAction<String> action =
                    new TransformChainAction<>(
                            1L,
                            "startup-test",
                            Collections.emptySet(),
                            Collections.emptySet(),
                            new ArrayList<>(children));
            flow = new TransformFlowLifeCycle<>(action, task, collector, new CompletableFuture<>());
        }
    }
}
