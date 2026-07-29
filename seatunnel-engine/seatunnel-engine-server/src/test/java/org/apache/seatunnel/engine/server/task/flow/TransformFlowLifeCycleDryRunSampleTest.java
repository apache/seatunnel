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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies bounded sample reporting across each stage in a transform chain. */
class TransformFlowLifeCycleDryRunSampleTest {

    @Test
    void shouldApplyAndCountEveryTransformStage() throws Exception {
        SeaTunnelMapTransform<String> first = new AppendingMapTransform("-first");
        SeaTunnelMapTransform<String> second = new AppendingMapTransform("-second");

        TransformChainAction<String> action =
                new TransformChainAction<>(
                        1L,
                        "sample-chain",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Arrays.asList(first, second));
        TransformFlowLifeCycle<String> flow =
                new TransformFlowLifeCycle<>(
                        action,
                        new TestSeaTunnelTask(),
                        new Collector<Record<?>>() {
                            @Override
                            public void collect(Record<?> record) {}

                            @Override
                            public void close() {}
                        },
                        new CompletableFuture<>());
        setField(flow, "dryRunSampleEnabled", true);
        setField(flow, "dryRunSamplePrintData", true);
        setField(flow, "dryRunSampleLimit", 1);

        assertEquals(Collections.singletonList("row-first-second"), flow.transform("row"));
        assertEquals(Collections.singletonList("row-first-second"), flow.transform("row"));

        assertArrayEquals(new int[] {1, 1}, (int[]) getField(flow, "dryRunSampleCounts"));
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class AppendingMapTransform implements SeaTunnelMapTransform<String> {
        private final String suffix;

        private AppendingMapTransform(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String getPluginName() {
            return "sample";
        }

        @Override
        public String map(String row) {
            return row + suffix;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }
    }

    private static class TestSeaTunnelTask extends SeaTunnelTask {
        private TestSeaTunnelTask() {
            super(1L, new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0), 0, null);
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
        protected void collect() {}

        @Override
        public Set<URL> getJarsUrl() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConnectorJarIdentifier> getConnectorPluginJars() {
            return Collections.emptySet();
        }

        @Override
        public ProgressState call() {
            throw new UnsupportedOperationException();
        }
    }
}
