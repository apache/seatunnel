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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AutoscalerRuntimeConfigTest {

    @Test
    void defaultsToDisabledAndSafeRuntimeValues() {
        AutoscalerRuntimeConfig config = AutoscalerRuntimeConfig.defaults();

        Assertions.assertFalse(config.isEnabled());
        Assertions.assertTrue(config.getEvaluationIntervalSeconds() > 0);
        Assertions.assertTrue(config.getMaxMetricStalenessSeconds() > 0);
        Assertions.assertTrue(config.getMinWorkers() > 0);
        Assertions.assertTrue(config.getMaxWorkers() >= config.getMinWorkers());
    }

    @Test
    void rejectsInvalidWorkerBounds() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AutoscalerRuntimeConfig.builder().minWorkers(3).maxWorkers(2).build());
    }

    @Test
    void rejectsNonPositiveStabilizationWindows() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AutoscalerRuntimeConfig.builder().scaleOutStabilizationSeconds(0).build());
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AutoscalerRuntimeConfig.builder().scaleInStabilizationSeconds(-1).build());
    }
}
