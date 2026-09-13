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

package org.apache.seatunnel.engine.server.autoscale;

import org.apache.seatunnel.engine.common.config.server.AutoscalerConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultAutoScalerTest {

    @Test
    void publishesNoActionUntilScaleOutStabilizesThenClampsTarget() {
        AutoscalerConfig config = new AutoscalerConfig();
        config.setScaleOutStabilizationSeconds(300);
        config.setScaleStep(2);
        config.setMaxWorkers(4);
        FakeTimeSource timeSource = new FakeTimeSource(1_000L, 0L);
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10);
        DefaultAutoScaler autoscaler =
                new DefaultAutoScaler(
                        7L,
                        config,
                        () -> baseSnapshot().currentWorkers(3).cpu(MetricValue.valid(0.9d)).build(),
                        new HierarchicalAutoscalingPolicy(DefaultAutoScaler.policyConfig(config)),
                        new StabilizationTracker(300_000_000_000L, 600_000_000_000L),
                        store,
                        timeSource);

        autoscaler.evaluateOnce();
        Assertions.assertEquals(
                ScalingAction.NO_ACTION,
                store.view(true, true).getLatestRecommendation().getAction());

        timeSource.nanos = 300_000_000_000L;
        autoscaler.evaluateOnce();

        ScalingRecommendation recommendation = store.view(true, true).getLatestRecommendation();
        Assertions.assertEquals(ScalingAction.SCALE_OUT, recommendation.getAction());
        Assertions.assertEquals(4, recommendation.getRecommendedWorkers());
        Assertions.assertEquals(7L, recommendation.getMasterEpoch());
        Assertions.assertEquals(1L, recommendation.getGeneration());
    }

    @Test
    void resetClearsGenerationAndStabilization() {
        AutoscalerConfig config = new AutoscalerConfig();
        config.setScaleOutStabilizationSeconds(0);
        FakeTimeSource timeSource = new FakeTimeSource(1_000L, 0L);
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10);
        DefaultAutoScaler autoscaler =
                new DefaultAutoScaler(
                        8L,
                        config,
                        () -> baseSnapshot().cpu(MetricValue.valid(0.9d)).build(),
                        new HierarchicalAutoscalingPolicy(DefaultAutoScaler.policyConfig(config)),
                        new StabilizationTracker(0L, 0L),
                        store,
                        timeSource);

        autoscaler.evaluateOnce();
        autoscaler.reset(9L);
        autoscaler.evaluateOnce();

        ScalingRecommendation recommendation = store.view(true, true).getLatestRecommendation();
        Assertions.assertEquals(9L, recommendation.getMasterEpoch());
        Assertions.assertEquals(0L, recommendation.getGeneration());
    }

    private AutoscalerMetricsSnapshot.Builder baseSnapshot() {
        return AutoscalerMetricsSnapshot.builder()
                .evaluationTimeMillis(1_000L)
                .currentWorkers(3)
                .minWorkers(1)
                .maxWorkers(10)
                .slotMode(SlotMode.FIXED)
                .fixedSlotUtilization(MetricValue.valid(0.1d))
                .cpu(MetricValue.valid(0.1d))
                .jvmMemory(MetricValue.valid(0.1d))
                .scaleInMetricsValid(true);
    }

    private static final class FakeTimeSource implements AutoscalerTimeSource {
        private long millis;
        private long nanos;

        private FakeTimeSource(long millis, long nanos) {
            this.millis = millis;
            this.nanos = nanos;
        }

        @Override
        public long currentTimeMillis() {
            return millis;
        }

        @Override
        public long nanoTime() {
            return nanos;
        }
    }
}
