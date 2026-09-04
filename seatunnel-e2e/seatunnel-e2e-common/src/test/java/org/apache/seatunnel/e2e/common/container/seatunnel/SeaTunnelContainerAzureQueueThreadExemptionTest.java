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

package org.apache.seatunnel.e2e.common.container.seatunnel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeaTunnelContainerAzureQueueThreadExemptionTest {

    @AfterEach
    void clearExemption() {
        SeaTunnelContainer.disableAzureQueueReactorThreadExemption();
    }

    @Test
    void shadedReactorNettyThreadIsAlwaysExempt() {
        assertTrue(
                isAzureQueueReactorThreadExempt(
                        "org.apache.seatunnel.shade.azure.queue.reactor-http-nio-1"));
    }

    @Test
    void boundedElasticThreadIsNotExemptOutsideAzureQueueTest() {
        assertFalse(isAzureQueueReactorThreadExempt("boundedElastic-evictor-1"));
    }

    @Test
    void boundedElasticThreadIsExemptDuringAzureQueueTest() {
        SeaTunnelContainer.enableAzureQueueReactorThreadExemption();

        assertTrue(isAzureQueueReactorThreadExempt("boundedElastic-evictor-1"));
    }

    @Test
    void unrelatedBoundedElasticThreadNameIsNotExempt() {
        SeaTunnelContainer.enableAzureQueueReactorThreadExemption();

        assertFalse(isAzureQueueReactorThreadExempt("boundedElastic-worker-1"));
    }

    private static boolean isAzureQueueReactorThreadExempt(String threadName) {
        return SeaTunnelContainer.isAzureQueueReactorThreadExempt(threadName);
    }
}
