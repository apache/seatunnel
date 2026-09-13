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

package org.apache.seatunnel.engine.common.config;

import org.apache.seatunnel.engine.common.config.server.AutoscalerConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

class YamlSeaTunnelDomConfigProcessorAutoscalerTest {

    @TempDir private Path tempDir;

    @Test
    void absentAutoscalerBlockUsesDisabledDefaults() throws Exception {
        AutoscalerConfig config =
                parse("seatunnel:\n" + "  engine:\n" + "    backup-count: 1\n")
                        .getEngineConfig()
                        .getAutoscalerConfig();

        Assertions.assertFalse(config.isEnabled());
        Assertions.assertEquals(30, config.getEvaluationIntervalSeconds());
        Assertions.assertEquals(120, config.getMetricsFreshnessSeconds());
        Assertions.assertEquals(5, config.getMaxFutureSkewSeconds());
        Assertions.assertEquals(300, config.getScaleOutStabilizationSeconds());
        Assertions.assertEquals(600, config.getScaleInStabilizationSeconds());
        Assertions.assertEquals(0.8d, config.getScaleOutCpuThreshold());
        Assertions.assertEquals(0.8d, config.getScaleOutJvmMemoryThreshold());
        Assertions.assertEquals(0.3d, config.getScaleInCpuThreshold());
        Assertions.assertEquals(0.3d, config.getScaleInJvmMemoryThreshold());
        Assertions.assertEquals(0.8d, config.getFixedSlotScaleOutThreshold());
        Assertions.assertEquals(0.3d, config.getFixedSlotScaleInThreshold());
        Assertions.assertEquals(1, config.getScaleStep());
        Assertions.assertEquals(1, config.getMinWorkers());
        Assertions.assertEquals(Integer.MAX_VALUE, config.getMaxWorkers());
        Assertions.assertEquals(20, config.getHistorySize());
    }

    @Test
    void parsesExplicitAutoscalerBlock() throws Exception {
        AutoscalerConfig config =
                parse(
                                "seatunnel:\n"
                                        + "  engine:\n"
                                        + "    autoscaler:\n"
                                        + "      enabled: true\n"
                                        + "      evaluation-interval-seconds: 11\n"
                                        + "      metrics-freshness-seconds: 22\n"
                                        + "      max-future-skew-seconds: 3\n"
                                        + "      scale-out-stabilization-seconds: 44\n"
                                        + "      scale-in-stabilization-seconds: 55\n"
                                        + "      scale-out-cpu-threshold: 0.91\n"
                                        + "      scale-out-jvm-memory-threshold: 0.92\n"
                                        + "      scale-in-cpu-threshold: 0.21\n"
                                        + "      scale-in-jvm-memory-threshold: 0.22\n"
                                        + "      fixed-slot-scale-out-threshold: 0.93\n"
                                        + "      fixed-slot-scale-in-threshold: 0.23\n"
                                        + "      scale-step: 2\n"
                                        + "      min-workers: 3\n"
                                        + "      max-workers: 7\n"
                                        + "      history-size: 9\n")
                        .getEngineConfig()
                        .getAutoscalerConfig();

        Assertions.assertTrue(config.isEnabled());
        Assertions.assertEquals(11, config.getEvaluationIntervalSeconds());
        Assertions.assertEquals(22, config.getMetricsFreshnessSeconds());
        Assertions.assertEquals(3, config.getMaxFutureSkewSeconds());
        Assertions.assertEquals(44, config.getScaleOutStabilizationSeconds());
        Assertions.assertEquals(55, config.getScaleInStabilizationSeconds());
        Assertions.assertEquals(0.91d, config.getScaleOutCpuThreshold());
        Assertions.assertEquals(0.92d, config.getScaleOutJvmMemoryThreshold());
        Assertions.assertEquals(0.21d, config.getScaleInCpuThreshold());
        Assertions.assertEquals(0.22d, config.getScaleInJvmMemoryThreshold());
        Assertions.assertEquals(0.93d, config.getFixedSlotScaleOutThreshold());
        Assertions.assertEquals(0.23d, config.getFixedSlotScaleInThreshold());
        Assertions.assertEquals(2, config.getScaleStep());
        Assertions.assertEquals(3, config.getMinWorkers());
        Assertions.assertEquals(7, config.getMaxWorkers());
        Assertions.assertEquals(9, config.getHistorySize());
    }

    @Test
    void rejectsInvalidThresholdOrdering() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        parse(
                                "seatunnel:\n"
                                        + "  engine:\n"
                                        + "    autoscaler:\n"
                                        + "      scale-out-cpu-threshold: 0.2\n"
                                        + "      scale-in-cpu-threshold: 0.3\n"));
    }

    @Test
    void rejectsInvalidWorkerRange() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        parse(
                                "seatunnel:\n"
                                        + "  engine:\n"
                                        + "    autoscaler:\n"
                                        + "      min-workers: 3\n"
                                        + "      max-workers: 2\n"));
    }

    private SeaTunnelConfig parse(String yaml) throws Exception {
        Path configFile = tempDir.resolve("seatunnel.yaml");
        Files.write(configFile, yaml.getBytes(StandardCharsets.UTF_8));
        return new YamlSeaTunnelConfigBuilder(new FileInputStream(configFile.toFile())).build();
    }
}
