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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

public class SourceSplitEnumeratorTaskTest {

    @Test
    void testOpenShouldBeforeReaderRegister() throws Exception {

        SeaTunnelSource source = Mockito.mock(SeaTunnelSource.class);
        SourceSplitEnumerator enumerator = Mockito.mock(SourceSplitEnumerator.class);
        Mockito.when(source.createEnumerator(Mockito.any())).thenReturn(enumerator);

        AtomicLong openTime = new AtomicLong(0);
        Mockito.doAnswer(
                        answer -> {
                            openTime.set(System.currentTimeMillis());
                            return null;
                        })
                .when(enumerator)
                .open();

        AtomicLong registerReaderTime = new AtomicLong(0);
        Mockito.doAnswer(
                        answer -> {
                            registerReaderTime.set(System.currentTimeMillis());
                            return null;
                        })
                .when(enumerator)
                .registerReader(Mockito.anyInt());

        SourceAction action =
                new SourceAction<>(1, "fake", source, new HashSet<>(), Collections.emptySet());
        SourceSplitEnumeratorTask enumeratorTask =
                new SourceSplitEnumeratorTask<>(
                        1, new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1), action);

        TaskExecutionContext context = Mockito.mock(TaskExecutionContext.class);
        InvocationFuture future = Mockito.mock(InvocationFuture.class);
        Mockito.when(context.getOrCreateMetricsContext(Mockito.any())).thenReturn(null);
        Mockito.when(context.sendToMaster(Mockito.any())).thenReturn(future);
        Mockito.when(future.join()).thenReturn(null);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(context.getTaskExecutionService()).thenReturn(taskExecutionService);

        enumeratorTask.setTaskExecutionContext(context);

        // re-order the method call to test the open() should be called before receivedReader()
        CompletableFuture.runAsync(
                () -> {
                    try {
                        Thread.sleep(1000);
                        enumeratorTask.receivedReader(
                                new TaskLocation(new TaskGroupLocation(1, 1, 1), 1, 1),
                                Address.createUnresolvedAddress("localhost", 5701));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        enumeratorTask.init();
        enumeratorTask.restoreState(new ArrayList<>());

        while (openTime.get() == 0 || registerReaderTime.get() == 0) {
            enumeratorTask.call();
        }

        Assertions.assertTrue(openTime.get() < registerReaderTime.get());
    }
}
