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

class SeaTunnelContainerAzureSdkThreadExemptionTest {

    @AfterEach
    void clearExemption() {
        SeaTunnelContainer.azureSdkReactorE2eCount.set(0);
    }

    @Test
    void shadedAzureQueueReactorNettyThreadIsAlwaysExempt() {
        assertTrue(
                isAzureSdkReactorThreadExempt(
                        "org.apache.seatunnel.shade.azure.queue.reactor-http-nio-1"));
    }

    @Test
    void boundedElasticEvictorIsNotExemptOutsideAzureSdkTest() {
        assertFalse(isAzureSdkReactorThreadExempt("boundedElastic-evictor-1"));
    }

    @Test
    void boundedElasticEvictorIsExemptDuringAzureSdkTest() {
        SeaTunnelContainer.enableAzureSdkReactorThreadExemption();

        assertTrue(isAzureSdkReactorThreadExempt("boundedElastic-evictor-1"));
    }

    @Test
    void azureSdkStaticWorkersAreExemptDuringAzureSdkTest() {
        SeaTunnelContainer.enableAzureSdkReactorThreadExemption();

        assertTrue(isAzureSdkReactorThreadExempt("boundedElastic-1"));
        assertTrue(isAzureSdkReactorThreadExempt("parallel-12"));
        assertTrue(isAzureSdkReactorThreadExempt("receiverPump-3"));
    }

    @Test
    void azureSdkStaticWorkersAreNotExemptOutsideAzureSdkTest() {
        assertFalse(isAzureSdkReactorThreadExempt("boundedElastic-1"));
        assertFalse(isAzureSdkReactorThreadExempt("parallel-12"));
        assertFalse(isAzureSdkReactorThreadExempt("receiverPump-3"));
    }

    @Test
    void similarlyNamedThreadsAreNotExempt() {
        SeaTunnelContainer.enableAzureSdkReactorThreadExemption();

        assertFalse(isAzureSdkReactorThreadExempt("boundedElastic-worker-1"));
        assertFalse(isAzureSdkReactorThreadExempt("parallel-worker"));
        assertFalse(isAzureSdkReactorThreadExempt("receiverPump-worker"));
    }

    @Test
    void overlappingAzureSdkTestsKeepExemptionUntilAllComplete() {
        SeaTunnelContainer.enableAzureSdkReactorThreadExemption();
        SeaTunnelContainer.enableAzureSdkReactorThreadExemption();

        SeaTunnelContainer.disableAzureSdkReactorThreadExemption();
        assertTrue(isAzureSdkReactorThreadExempt("receiverPump-1"));

        SeaTunnelContainer.disableAzureSdkReactorThreadExemption();
        assertFalse(isAzureSdkReactorThreadExempt("receiverPump-1"));
    }

    @Test
    void extraDisableDoesNotActivateOrUnderflowExemption() {
        SeaTunnelContainer.disableAzureSdkReactorThreadExemption();

        assertFalse(isAzureSdkReactorThreadExempt("receiverPump-1"));
        assertTrue(SeaTunnelContainer.azureSdkReactorE2eCount.get() >= 0);
    }

    private static boolean isAzureSdkReactorThreadExempt(String threadName) {
        return SeaTunnelContainer.isAzureSdkReactorThreadExempt(threadName);
    }
}
