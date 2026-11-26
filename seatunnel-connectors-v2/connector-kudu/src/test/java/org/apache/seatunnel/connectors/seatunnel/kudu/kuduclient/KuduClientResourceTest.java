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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.util.KuduUtil;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * Flink runs its JobManager and TaskManager inside a Subject without Kerberos credentials. Kudu
     * ignores such a Subject, but it still calls Subject#toString on it while holding the Subject's
     * principal set lock. On JDK 11 that call can deadlock with any thread being created under the
     * same Subject, which froze the Flink JobManager in the Kudu E2E tests.
     */
    @Test
    void shouldNotInspectCallerSubjectWithoutKerberosCredentials() throws Exception {
        AtomicInteger toStringCalls = new AtomicInteger();
        Principal principal =
                new Principal() {
                    @Override
                    public String getName() {
                        return "flink";
                    }

                    @Override
                    public String toString() {
                        toStringCalls.incrementAndGet();
                        return getName();
                    }
                };
        Subject subject =
                new Subject(
                        false,
                        Collections.singleton(principal),
                        Collections.emptySet(),
                        Collections.emptySet());
        CommonConfig config =
                new CommonConfig(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap("kudu_masters", "localhost:7051")));

        KuduClientResource resource =
                Subject.doAs(
                        subject,
                        (PrivilegedExceptionAction<KuduClientResource>)
                                () -> KuduUtil.getKuduClientResource(config));
        resource.close();

        Assertions.assertEquals(0, toStringCalls.get());
    }

    @Test
    void shouldPassCallerSubjectWithKerberosPrincipalToKudu() throws Exception {
        Subject subject =
                new Subject(
                        false,
                        Collections.singleton(new KerberosPrincipal("seatunnel@EXAMPLE.COM")),
                        Collections.emptySet(),
                        Collections.emptySet());

        KuduClientResource resource =
                Subject.doAs(
                        subject,
                        (PrivilegedExceptionAction<KuduClientResource>)
                                () -> KuduUtil.getKuduClientResource(localConfig()));
        try {
            Assertions.assertSame(subject, kuduSubject(resource.getClient()));
        } finally {
            resource.close();
        }
    }

    private static CommonConfig localConfig() {
        return new CommonConfig(
                ReadonlyConfig.fromMap(Collections.singletonMap("kudu_masters", "localhost:7051")));
    }

    private static Subject kuduSubject(KuduClient client) throws Exception {
        Field asyncClientField = KuduClient.class.getDeclaredField("asyncClient");
        asyncClientField.setAccessible(true);
        Object asyncClient = asyncClientField.get(client);
        Field securityContextField = asyncClient.getClass().getDeclaredField("securityContext");
        securityContextField.setAccessible(true);
        Object securityContext = securityContextField.get(asyncClient);
        Method getSubject = securityContext.getClass().getDeclaredMethod("getSubject");
        getSubject.setAccessible(true);
        return (Subject) getSubject.invoke(securityContext);
    }
}
