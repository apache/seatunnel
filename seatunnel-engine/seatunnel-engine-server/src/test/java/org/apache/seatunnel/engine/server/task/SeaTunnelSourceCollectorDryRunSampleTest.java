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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.common.metrics.ThreadSafeQPSMeter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;
import org.apache.seatunnel.engine.common.config.DryRunSampleConfig;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SeaTunnelSourceCollectorDryRunSampleTest {

    @Test
    void shouldForwardOnlyTheConfiguredNumberOfRowsAndCompleteOnce() throws Exception {
        RecordingOutput output = new RecordingOutput();
        AtomicInteger completionCount = new AtomicInteger();
        JobConfig jobConfig = new JobConfig();
        DryRunSampleConfig.configure(jobConfig, 2, false);
        DryRunSampleConfig.applyTrustedConfiguration(jobConfig);
        SeaTunnelSourceCollector<String> collector =
                new SeaTunnelSourceCollector<>(
                        new Object(),
                        Collections.singletonList(output),
                        metricsContext(),
                        FlowControlStrategy.builder().build(),
                        BasicType.STRING_TYPE,
                        Collections.emptyList(),
                        sourceTask(),
                        new EngineConfig(),
                        jobConfig.getEnvOptions(),
                        completionCount::incrementAndGet);

        collector.collect("first");
        collector.collect("second");
        collector.collect("ignored");

        assertEquals(2, output.records.size());
        assertEquals(1, completionCount.get());
    }

    private static SeaTunnelTask sourceTask() {
        return new TestSeaTunnelTask();
    }

    private static MetricsContext metricsContext() {
        return new TestMetricsContext();
    }

    private static class RecordingOutput implements OneInputFlowLifeCycle<Record<?>> {
        private final List<Record<?>> records = new ArrayList<>();

        @Override
        public void received(Record<?> record) {
            records.add(record);
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

    private static class TestMetricsContext implements MetricsContext {
        @Override
        public Counter counter(String name) {
            return new ThreadSafeCounter(name);
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public Meter meter(String name) {
            return new ThreadSafeQPSMeter(name);
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }
    }
}
