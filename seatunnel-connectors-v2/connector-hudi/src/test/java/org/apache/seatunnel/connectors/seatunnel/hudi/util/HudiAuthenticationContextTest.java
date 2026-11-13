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

package org.apache.seatunnel.connectors.seatunnel.hudi.util;

import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class HudiAuthenticationContextTest {

    @Test
    void disabledAuthenticationPreservesCallerAndExceptions() throws Exception {
        UserGroupInformation caller = UserGroupInformation.getCurrentUser();
        HudiAuthenticationContext context =
                new HudiAuthenticationContext(new Configuration(false), null, null, null);
        assertEquals(caller, context.execute(UserGroupInformation::getCurrentUser));
        IllegalStateException expected = new IllegalStateException("original failure");
        assertSame(
                expected,
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                context.execute(
                                        () -> {
                                            throw expected;
                                        })));
        context.close(() -> null);
        context.close(
                () -> {
                    fail("Cleanup must be idempotent");
                    return null;
                });
        assertThrows(IllegalStateException.class, () -> context.execute(() -> null));
    }

    @Test
    void rejectsIncompleteOrBlankCredentialPairs() {
        for (String[] pair :
                Arrays.asList(
                        new String[] {"principal", null},
                        new String[] {null, "keytab"},
                        new String[] {" ", "keytab"},
                        new String[] {"principal", ""},
                        new String[] {"", ""})) {
            assertThrows(
                    HudiConnectorException.class,
                    () ->
                            new HudiAuthenticationContext(
                                    new Configuration(false), pair[0], pair[1], null));
        }
    }

    @Test
    void rejectsCredentialsWithNonKerberosHadoopConfiguration() {
        HudiConnectorException error =
                assertThrows(
                        HudiConnectorException.class,
                        () ->
                                new HudiAuthenticationContext(
                                        new Configuration(false), "principal", "keytab", null));
        assertTrue(error.getMessage().contains("hadoop.security.authentication=kerberos"));
        assertNull(error.getCause());
    }

    @Test
    void sanitizesLoginFailure() throws Exception {
        try (MockedStatic<UserGroupInformation> login =
                Mockito.mockStatic(UserGroupInformation.class)) {
            login.when(() -> UserGroupInformation.setConfiguration(any())).thenAnswer(i -> null);
            login.when(
                            () ->
                                    UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                                            anyString(), anyString()))
                    .thenThrow(new IOException("sensitive-principal sensitive-keytab"));
            HudiConnectorException error =
                    assertThrows(
                            HudiConnectorException.class,
                            () ->
                                    new HudiAuthenticationContext(
                                            kerberosConfiguration(),
                                            "sensitive-principal",
                                            "sensitive-keytab",
                                            null));
            assertFalse(error.toString().contains("sensitive-"));
            assertNull(error.getCause());
        }
    }

    @Test
    void scopesHudiParallelStreamsAndNewExecutorThreadsWithoutChangingCaller() throws Exception {
        UserGroupInformation caller = UserGroupInformation.getCurrentUser();
        ForkJoinPool.commonPool().submit(() -> IntStream.range(0, 1000).parallel().sum()).get();
        UserGroupInformation user = spy(UserGroupInformation.createRemoteUser("hudi-owned"));
        HudiAuthenticationContext context = authenticated(user);
        try {
            List<Integer> input = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
            List<String> mapped =
                    context.execute(
                            () -> {
                                HoodieJavaEngineContext engine =
                                        new HoodieJavaEngineContext(
                                                new HadoopStorageConfiguration(
                                                        context.getConfiguration()));
                                return engine.map(
                                        input,
                                        value ->
                                                UserGroupInformation.getCurrentUser().getUserName(),
                                        2);
                            });
            assertTrue(mapped.stream().allMatch("hudi-owned"::equals));
            List<String> data =
                    context.execute(
                            () ->
                                    HoodieListData.eager(input)
                                            .map(
                                                    value ->
                                                            UserGroupInformation.getCurrentUser()
                                                                    .getUserName())
                                            .collectAsList());
            assertTrue(data.stream().allMatch("hudi-owned"::equals));
            AtomicReference<UserGroupInformation> inherited = new AtomicReference<>();
            context.execute(
                    () -> {
                        Thread thread =
                                new CustomizedThreadFactory("hudi-test")
                                        .newThread(
                                                () -> {
                                                    try {
                                                        inherited.set(
                                                                UserGroupInformation
                                                                        .getCurrentUser());
                                                    } catch (IOException e) {
                                                        throw new AssertionError(e);
                                                    }
                                                });
                        thread.start();
                        thread.join();
                        return null;
                    });
            assertEquals(user.getUserName(), inherited.get().getUserName());
            assertEquals(caller, UserGroupInformation.getCurrentUser());
        } finally {
            context.close(
                    () -> {
                        assertEquals(
                                user.getUserName(),
                                UserGroupInformation.getCurrentUser().getUserName());
                        return null;
                    });
        }
        verify(user, atLeastOnce()).checkTGTAndReloginFromKeytab();
        verify(user).logoutUserFromKeytab();
    }

    @Test
    void renewalFailureStopsWorkButStillClosesUnderOriginalIdentity() throws Exception {
        UserGroupInformation user = spy(UserGroupInformation.createRemoteUser("hudi-renewal"));
        HudiAuthenticationContext context = authenticated(user);
        doThrow(new IOException("sensitive-principal sensitive-keytab"))
                .when(user)
                .checkTGTAndReloginFromKeytab();
        AtomicBoolean ran = new AtomicBoolean();
        HudiConnectorException error =
                assertThrows(
                        HudiConnectorException.class,
                        () ->
                                context.execute(
                                        () -> {
                                            ran.set(true);
                                            return null;
                                        }));
        assertFalse(ran.get());
        assertNull(error.getCause());
        assertFalse(error.toString().contains("sensitive-"));
        context.close(
                () -> {
                    assertEquals(
                            user.getUserName(),
                            UserGroupInformation.getCurrentUser().getUserName());
                    return null;
                });
        verify(user).logoutUserFromKeytab();
    }

    @Test
    void distinctContextsDoNotReuseIdentitiesAndCleanupFailureStillLogsOut() throws Exception {
        UserGroupInformation first = spy(UserGroupInformation.createRemoteUser("hudi-first"));
        UserGroupInformation second = spy(UserGroupInformation.createRemoteUser("hudi-second"));
        HudiAuthenticationContext one = authenticated(first);
        HudiAuthenticationContext two = authenticated(second);
        try {
            assertEquals(
                    first.getUserName(),
                    one.execute(() -> UserGroupInformation.getCurrentUser().getUserName()));
            assertEquals(
                    second.getUserName(),
                    two.execute(() -> UserGroupInformation.getCurrentUser().getUserName()));
            assertEquals(
                    first.getUserName(),
                    one.execute(() -> UserGroupInformation.getCurrentUser().getUserName()));
        } finally {
            assertThrows(
                    HudiConnectorException.class,
                    () ->
                            one.close(
                                    () -> {
                                        assertEquals(
                                                first.getUserName(),
                                                UserGroupInformation.getCurrentUser()
                                                        .getUserName());
                                        throw new IOException("sensitive-cleanup");
                                    }));
            two.close(() -> null);
        }
        verify(first).logoutUserFromKeytab();
        verify(second).logoutUserFromKeytab();
    }

    private static Configuration kerberosConfiguration() {
        Configuration configuration = new Configuration(false);
        configuration.set("hadoop.security.authentication", "kerberos");
        return configuration;
    }

    private static HudiAuthenticationContext authenticated(UserGroupInformation user)
            throws IOException {
        try (MockedStatic<UserGroupInformation> login =
                Mockito.mockStatic(UserGroupInformation.class)) {
            login.when(() -> UserGroupInformation.setConfiguration(any())).thenAnswer(i -> null);
            login.when(
                            () ->
                                    UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                                            "principal", "keytab"))
                    .thenReturn(user);
            HudiAuthenticationContext context =
                    new HudiAuthenticationContext(
                            kerberosConfiguration(), "principal", "keytab", null);
            login.verify(() -> UserGroupInformation.setConfiguration(any()));
            login.verify(
                    () ->
                            UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                                    "principal", "keytab"));
            return context;
        }
    }
}
