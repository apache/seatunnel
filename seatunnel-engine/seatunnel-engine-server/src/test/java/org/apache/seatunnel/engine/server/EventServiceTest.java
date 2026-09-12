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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.engine.server.event.JobEventReportOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Covers event forwarding retries so each send attempt uses a fresh Hazelcast operation.
 *
 * <p>The regression assertions prevent retries from reusing an already executed operation.
 */
public class EventServiceTest {

    @Test
    public void testRetryUsesFreshJobEventReportOperationInstance() throws Exception {
        Address masterAddress = new Address("localhost", 5801);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        MemberImpl masterMember = Mockito.mock(MemberImpl.class);
        OperationServiceImpl operationService = Mockito.mock(OperationServiceImpl.class);
        InvocationBuilder invocationBuilder = Mockito.mock(InvocationBuilder.class);
        InvocationFuture<Void> failedInvocation = Mockito.mock(InvocationFuture.class);
        InvocationFuture<Void> successInvocation = Mockito.mock(InvocationFuture.class);
        Mockito.when(failedInvocation.join()).thenThrow(new IllegalStateException("retry"));
        Mockito.when(successInvocation.join()).thenReturn(null);
        Mockito.when(nodeEngine.getMasterAddress()).thenReturn(masterAddress);
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMember(masterAddress)).thenReturn(masterMember);
        Mockito.when(masterMember.isLiteMember()).thenReturn(false);
        Mockito.when(nodeEngine.getOperationService()).thenReturn(operationService);
        Mockito.when(invocationBuilder.setAsync()).thenReturn(invocationBuilder);

        CountDownLatch attempts = new CountDownLatch(2);
        List<Operation> capturedOperations = new CopyOnWriteArrayList<>();

        Mockito.when(
                        operationService.createInvocationBuilder(
                                Mockito.anyString(),
                                Mockito.any(Operation.class),
                                Mockito.eq(masterAddress)))
                .thenAnswer(
                        invocation -> {
                            Operation operation = invocation.getArgument(1);
                            capturedOperations.add(operation);
                            attempts.countDown();
                            return invocationBuilder;
                        });
        Mockito.when(invocationBuilder.invoke())
                .thenAnswer(
                        invocation ->
                                capturedOperations.size() == 1
                                        ? failedInvocation
                                        : successInvocation);

        EventService eventService = new EventService(nodeEngine);
        try {
            eventService.reportEvent(Mockito.mock(Event.class));

            Assertions.assertTrue(attempts.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(2, capturedOperations.size());
            Assertions.assertInstanceOf(JobEventReportOperation.class, capturedOperations.get(0));
            Assertions.assertInstanceOf(JobEventReportOperation.class, capturedOperations.get(1));
            Assertions.assertNotSame(capturedOperations.get(0), capturedOperations.get(1));
        } finally {
            eventService.shutdownNow();
        }
    }
}
