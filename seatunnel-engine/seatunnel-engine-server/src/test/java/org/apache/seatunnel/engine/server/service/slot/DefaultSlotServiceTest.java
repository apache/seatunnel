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

package org.apache.seatunnel.engine.server.service.slot;

import org.apache.seatunnel.engine.common.config.server.AllocateStrategy;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.autoscale.AutoscalerRuntimeConfig;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ReportAutoscalerMetricsOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.WorkerHeartbeatOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;

class DefaultSlotServiceTest {

    @Test
    void disabledAutoscalerDoesNotReportAutoscalerMetrics() throws Exception {
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        MemberImpl localMember = Mockito.mock(MemberImpl.class);
        Address localAddress = new Address("127.0.0.1", 5801);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(hazelcastInstance.getName()).thenReturn("default-slot-service-test");
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getThisAddress()).thenReturn(localAddress);
        Mockito.when(nodeEngine.getThisAddress()).thenReturn(localAddress);
        Mockito.when(nodeEngine.getLocalMember()).thenReturn(localMember);
        Mockito.when(localMember.getAttributes()).thenReturn(Collections.emptyMap());

        SlotServiceConfig slotServiceConfig = new SlotServiceConfig();
        slotServiceConfig.setAllocateStrategy(AllocateStrategy.SYSTEM_LOAD);
        AutoscalerRuntimeConfig autoscalerConfig = AutoscalerRuntimeConfig.defaults();
        CapturingSlotService slotService =
                new CapturingSlotService(
                        nodeEngine,
                        Mockito.mock(TaskExecutionService.class),
                        slotServiceConfig,
                        autoscalerConfig);
        try {
            slotService.init();

            await().untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            slotService.countOperations(
                                                            WorkerHeartbeatOperation.class)
                                                    >= 2));

            Assertions.assertEquals(
                    0,
                    slotService.countOperations(ReportAutoscalerMetricsOperation.class),
                    "Disabled autoscaler must not report worker metrics to master");
        } finally {
            slotService.close();
        }
    }

    @Test
    void enabledAutoscalerReportsWorkerMetricsAlongsideHeartbeat() throws Exception {
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        MemberImpl localMember = Mockito.mock(MemberImpl.class);
        Address localAddress = new Address("127.0.0.1", 5802);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(hazelcastInstance.getName()).thenReturn("enabled-slot-service-test");
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getThisAddress()).thenReturn(localAddress);
        Mockito.when(nodeEngine.getThisAddress()).thenReturn(localAddress);
        Mockito.when(nodeEngine.getLocalMember()).thenReturn(localMember);
        Mockito.when(localMember.getAttributes()).thenReturn(Collections.emptyMap());

        SlotServiceConfig slotServiceConfig = new SlotServiceConfig();
        slotServiceConfig.setAllocateStrategy(AllocateStrategy.SYSTEM_LOAD);
        AutoscalerRuntimeConfig autoscalerConfig =
                AutoscalerRuntimeConfig.builder().enabled(true).build();
        CapturingSlotService slotService =
                new CapturingSlotService(
                        nodeEngine,
                        Mockito.mock(TaskExecutionService.class),
                        slotServiceConfig,
                        autoscalerConfig);
        try {
            slotService.init();

            await().untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            slotService.countOperations(
                                                            ReportAutoscalerMetricsOperation.class)
                                                    >= 1));
            Assertions.assertTrue(
                    slotService.countOperations(WorkerHeartbeatOperation.class)
                            >= slotService.countOperations(ReportAutoscalerMetricsOperation.class));
        } finally {
            slotService.close();
        }
    }

    private static final class CapturingSlotService extends DefaultSlotService {

        private final List<Operation> operations = new CopyOnWriteArrayList<>();
        private final InvocationFuture<Object> completedFuture;

        private CapturingSlotService(
                NodeEngineImpl nodeEngine,
                TaskExecutionService taskExecutionService,
                SlotServiceConfig config,
                AutoscalerRuntimeConfig autoscalerConfig) {
            super(nodeEngine, taskExecutionService, config, autoscalerConfig);
            completedFuture = Mockito.mock(InvocationFuture.class);
            Mockito.when(completedFuture.join()).thenReturn(null);
        }

        @Override
        public <E> InvocationFuture<E> sendToMaster(Operation operation) {
            operations.add(operation);
            return (InvocationFuture<E>) completedFuture;
        }

        @Override
        public double getCpuPercentage() {
            return 0.42D;
        }

        @Override
        public double getMemPercentage() {
            return 0.24D;
        }

        private long countOperations(Class<? extends Operation> operationType) {
            return operations.stream().filter(operationType::isInstance).count();
        }
    }
}
