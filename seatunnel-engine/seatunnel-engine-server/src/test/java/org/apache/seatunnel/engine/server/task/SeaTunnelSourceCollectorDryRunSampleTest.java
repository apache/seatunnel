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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;
import org.apache.seatunnel.engine.common.config.DryRunSampleConfig;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeaTunnelSourceCollectorDryRunSampleTest {

    @Test
    void shouldForwardOnlyTheConfiguredNumberOfRowsAndCompleteOnce() throws Exception {
        OneInputFlowLifeCycle<Record<?>> output = Mockito.mock(OneInputFlowLifeCycle.class);
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

        verify(output, times(2)).received(Mockito.any());
        org.junit.jupiter.api.Assertions.assertEquals(1, completionCount.get());
    }

    private static SeaTunnelTask sourceTask() {
        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));
        return sourceTask;
    }

    private static MetricsContext metricsContext() {
        MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
        when(metricsContext.counter(Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
        when(metricsContext.meter(Mockito.anyString())).thenReturn(Mockito.mock(Meter.class));
        return metricsContext;
    }
}
