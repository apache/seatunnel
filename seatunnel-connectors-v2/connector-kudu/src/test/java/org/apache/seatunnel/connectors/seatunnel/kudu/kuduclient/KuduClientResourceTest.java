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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

class KuduClientResourceTest {

    @Test
    void shouldWaitForExecutorAfterClosingClient() throws Exception {
        KuduClient kuduClient = Mockito.mock(KuduClient.class);
        ExecutorService executorService = Mockito.mock(ExecutorService.class);
        Mockito.when(executorService.awaitTermination(20L, TimeUnit.SECONDS)).thenReturn(true);

        new KuduClientResource(kuduClient, executorService).close();

        InOrder inOrder = Mockito.inOrder(kuduClient, executorService);
        inOrder.verify(kuduClient).close();
        inOrder.verify(executorService).shutdown();
        inOrder.verify(executorService).awaitTermination(20L, TimeUnit.SECONDS);
        Mockito.verify(executorService, Mockito.never()).shutdownNow();
    }

    @Test
    void shouldForceShutdownWhenGracefulShutdownTimesOut() throws Exception {
        KuduClient kuduClient = Mockito.mock(KuduClient.class);
        ExecutorService executorService = Mockito.mock(ExecutorService.class);
        Mockito.when(executorService.awaitTermination(20L, TimeUnit.SECONDS)).thenReturn(false);
        Mockito.when(executorService.awaitTermination(5L, TimeUnit.SECONDS)).thenReturn(true);

        new KuduClientResource(kuduClient, executorService).close();

        InOrder inOrder = Mockito.inOrder(kuduClient, executorService);
        inOrder.verify(kuduClient).close();
        inOrder.verify(executorService).shutdown();
        inOrder.verify(executorService).awaitTermination(20L, TimeUnit.SECONDS);
        inOrder.verify(executorService).shutdownNow();
        inOrder.verify(executorService).awaitTermination(5L, TimeUnit.SECONDS);
    }

    @Test
    void shouldShutdownExecutorWhenClosingClientFails() throws Exception {
        KuduClient kuduClient = Mockito.mock(KuduClient.class);
        KuduException closeException = Mockito.mock(KuduException.class);
        Mockito.doThrow(closeException).when(kuduClient).close();
        ExecutorService executorService = Mockito.mock(ExecutorService.class);
        Mockito.when(executorService.awaitTermination(20L, TimeUnit.SECONDS)).thenReturn(true);

        KuduException actualException =
                Assertions.assertThrows(
                        KuduException.class,
                        () -> new KuduClientResource(kuduClient, executorService).close());

        Assertions.assertSame(closeException, actualException);
        Mockito.verify(executorService).shutdown();
        Mockito.verify(executorService).awaitTermination(20L, TimeUnit.SECONDS);
    }
}
