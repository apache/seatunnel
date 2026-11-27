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

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
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
    void alreadyInterruptedCallerStartsNoWorkButCanClose() throws Exception {
        UserGroupInformation user = spy(UserGroupInformation.createRemoteUser("hudi-cancelled"));
        HudiAuthenticationContext context = authenticated(user);
        AtomicBoolean ran = new AtomicBoolean();
        AtomicBoolean cleaned = new AtomicBoolean();
        Thread.currentThread().interrupt();
        try {
            assertThrows(
                    HudiConnectorException.class,
                    () ->
                            context.execute(
                                    () -> {
                                        ran.set(true);
                                        return null;
                                    }));
            context.close(
                    () -> {
                        assertFalse(Thread.currentThread().isInterrupted());
                        cleaned.set(true);
                        return null;
                    });
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            context.close(() -> null);
        }
        assertFalse(ran.get());
        assertTrue(cleaned.get());
        verify(user).logoutUserFromKeytab();
    }

    @Test
    void nestedWorkerCannotCloseItsOwnPool() throws Exception {
        HudiAuthenticationContext context =
                authenticated(spy(UserGroupInformation.createRemoteUser("hudi-nested-close")));
        try {
            assertTrue(
                    context.execute(
                            () -> {
                                assertThrows(
                                        IllegalStateException.class,
                                        () -> context.close(() -> null));
                                return context.execute(() -> true);
                            }));
        } finally {
            context.close(() -> null);
        }
    }

    @Test
    void interruptionDuringCleanupWaitsForCleanupAndPreservesFlag() throws Exception {
        UserGroupInformation user = spy(UserGroupInformation.createRemoteUser("hudi-cleanup-wait"));
        HudiAuthenticationContext context = authenticated(user);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread closer =
                new Thread(
                        () -> {
                            try (MockedStatic<org.apache.hadoop.fs.FileSystem> files =
                                    Mockito.mockStatic(org.apache.hadoop.fs.FileSystem.class)) {
                                files.when(
                                                () ->
                                                        org.apache.hadoop.fs.FileSystem
                                                                .closeAllForUGI(user))
                                        .thenAnswer(
                                                invocation -> {
                                                    assertFalse(
                                                            Thread.currentThread().isInterrupted(),
                                                            "Restore interruption after filesystem cleanup");
                                                    return null;
                                                });
                                context.close(
                                        () -> {
                                            entered.countDown();
                                            release.await();
                                            return null;
                                        });
                                interrupted.set(Thread.currentThread().isInterrupted());
                            } catch (Throwable error) {
                                failure.set(error);
                            } finally {
                                done.countDown();
                            }
                        });
        closer.start();
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.interrupt();
            assertFalse(done.await(200, TimeUnit.MILLISECONDS));
            verify(user, Mockito.never()).logoutUserFromKeytab();
        } finally {
            release.countDown();
            closer.join(5_000);
            context.close(() -> null);
        }
        assertFalse(closer.isAlive());
        assertNull(failure.get());
        assertTrue(interrupted.get());
        verify(user).logoutUserFromKeytab();
    }

    @Test
    void interruptedCallerWaitsForOwnedOperationBeforeReportingFailure() throws Exception {
        UserGroupInformation user = spy(UserGroupInformation.createRemoteUser("hudi-interrupt"));
        HudiAuthenticationContext context = authenticated(user);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                context.execute(
                                        () -> {
                                            entered.countDown();
                                            release.await();
                                            return null;
                                        });
                            } catch (Throwable error) {
                                failure.set(error);
                                interrupted.set(Thread.currentThread().isInterrupted());
                            } finally {
                                finished.countDown();
                            }
                        });
        caller.start();
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            caller.interrupt();
            assertFalse(
                    finished.await(200, TimeUnit.MILLISECONDS),
                    "The caller must retain ownership until the action has settled");
        } finally {
            release.countDown();
            caller.join(5_000);
            context.close(() -> null);
        }
        assertFalse(caller.isAlive());
        assertTrue(failure.get() instanceof HudiConnectorException);
        assertTrue(interrupted.get());
        verify(user).logoutUserFromKeytab();
    }

    @Test
    void concurrentCloseDoesNotCleanUpAnActiveOperation() throws Exception {
        UserGroupInformation user = spy(UserGroupInformation.createRemoteUser("hudi-close"));
        HudiAuthenticationContext context = authenticated(user);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                context.execute(
                                        () -> {
                                            entered.countDown();
                                            release.await();
                                            return null;
                                        });
                            } catch (Throwable error) {
                                failure.set(error);
                            }
                        });
        Thread closer =
                new Thread(
                        () -> {
                            closeEntered.countDown();
                            try {
                                context.close(
                                        () -> {
                                            cleaned.countDown();
                                            return null;
                                        });
                            } catch (Throwable error) {
                                failure.set(error);
                            }
                        });
        caller.start();
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            assertTrue(closeEntered.await(5, TimeUnit.SECONDS));
            assertFalse(
                    cleaned.await(200, TimeUnit.MILLISECONDS),
                    "Client cleanup must wait for the admitted operation");
        } finally {
            release.countDown();
            caller.join(5_000);
            closer.join(5_000);
            context.close(() -> null);
        }
        assertFalse(caller.isAlive());
        assertFalse(closer.isAlive());
        assertNull(failure.get());
        assertEquals(0, cleaned.getCount());
        verify(user).logoutUserFromKeytab();
    }

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
    void actionFailuresKeepSafeCategoriesWithoutBecomingLoginFailures() throws Exception {
        HudiAuthenticationContext context =
                authenticated(spy(UserGroupInformation.createRemoteUser("hudi-operation")));
        try {
            for (boolean nested : new boolean[] {false, true}) {
                HudiConnectorException error =
                        assertThrows(
                                HudiConnectorException.class,
                                () ->
                                        context.execute(
                                                () -> {
                                                    if (nested) {
                                                        return context.execute(
                                                                () -> {
                                                                    throw new IllegalArgumentException(
                                                                            "sensitive-schema",
                                                                            new IOException(
                                                                                    "sensitive-principal sensitive-keytab"));
                                                                });
                                                    }
                                                    throw new IOException(
                                                            "sensitive-principal sensitive-keytab");
                                                }));
                assertEquals(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                        error.getSeaTunnelErrorCode());
                assertTrue(error.getMessage().contains("IOException"));
                assertNull(error.getCause());
                StringWriter trace = new StringWriter();
                error.printStackTrace(new PrintWriter(trace));
                assertFalse(trace.toString().contains("sensitive-"));
            }
        } finally {
            context.close(() -> null);
        }
    }

    @Test
    void cleanupKeepsFirstFailureAndSanitizedLogoutFailure() throws Exception {
        UserGroupInformation user = spy(UserGroupInformation.createRemoteUser("hudi-cleanup"));
        HudiAuthenticationContext context = authenticated(user);
        doThrow(new IOException("sensitive-keytab")).when(user).logoutUserFromKeytab();
        HudiConnectorException error =
                assertThrows(
                        HudiConnectorException.class,
                        () ->
                                context.close(
                                        () -> {
                                            throw new IllegalArgumentException("sensitive-cleanup");
                                        }));
        assertTrue(error.getMessage().contains("IllegalArgumentException"));
        assertEquals(
                CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, error.getSeaTunnelErrorCode());
        assertEquals(1, error.getSuppressed().length);
        assertTrue(error.getSuppressed()[0].getMessage().contains("logout"));
        assertTrue(error.getSuppressed()[0].getMessage().contains("IOException"));
        StringWriter trace = new StringWriter();
        error.printStackTrace(new PrintWriter(trace));
        assertFalse(trace.toString().contains("sensitive-"));
        context.close(
                () -> {
                    fail("Already closed");
                    return null;
                });
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
