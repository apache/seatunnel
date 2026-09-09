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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SeaTunnelTaskCloseTest {

    @Test
    void shouldCloseRemainingLifecyclesAfterUncheckedFailure() {
        List<String> closed = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("first");
        TestTask task =
                new TestTask(
                        lifecycle(() -> closed.add("first")),
                        lifecycle(
                                () -> {
                                    closed.add("second");
                                    throw failure;
                                }),
                        lifecycle(() -> closed.add("third")));

        IllegalStateException thrown = assertThrows(IllegalStateException.class, task::close);
        IllegalStateException repeatedThrown =
                assertThrows(IllegalStateException.class, task::close);

        assertSame(failure, thrown);
        assertSame(failure, repeatedThrown);
        assertEquals(Arrays.asList("first", "second", "third", "first", "second", "third"), closed);
    }

    @Test
    void shouldPreserveFirstFailureAndSuppressLaterFailures() {
        IOException first = new IOException("first");
        IllegalArgumentException second = new IllegalArgumentException("second");
        TestTask task =
                new TestTask(
                        lifecycle(
                                () -> {
                                    throw first;
                                }),
                        lifecycle(
                                () -> {
                                    throw second;
                                }));

        IOException thrown = assertThrows(IOException.class, task::close);

        assertSame(first, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(second, thrown.getSuppressed()[0]);
    }

    private static FlowLifeCycle lifecycle(ThrowingClose close) {
        return new FlowLifeCycle() {
            @Override
            public void close() throws IOException {
                close.run();
            }
        };
    }

    @FunctionalInterface
    private interface ThrowingClose {
        void run() throws IOException;
    }

    private static final class TestTask extends SeaTunnelTask {
        private TestTask(FlowLifeCycle... lifecycles) {
            super(1L, null, 0, null, Collections.emptyMap());
            allCycles = Arrays.asList(lifecycles);
        }

        @Override
        protected SourceFlowLifeCycle<?, ?> createSourceFlowLifeCycle(
                SourceAction<?, ?, ?> sourceAction,
                SourceConfig config,
                CompletableFuture<Void> completableFuture,
                MetricsContext metricsContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void collect() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProgressState call() {
            throw new UnsupportedOperationException();
        }
    }
}
