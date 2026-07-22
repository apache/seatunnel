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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the Couchbase SDK thread-exemption routing fix (Blocker 2).
 *
 * <p>Before the fix, four Couchbase-specific thread names were exempted inside the <em>global</em>
 * {@code isSystemThread()} method. This meant any Reactor-based connector that leaked a scheduler
 * thread matching {@code parallel-\d+} would silently pass the E2E thread-leak check.
 *
 * <p>After the fix:
 *
 * <ul>
 *   <li>{@code isSystemThread()} must return {@code false} for all four Couchbase thread names.
 *   <li>{@code isIssueWeAlreadyKnow()} must return {@code true} for those same names.
 * </ul>
 *
 * <p>Both methods are accessed via reflection so the test does not depend on a live container,
 * Docker daemon, or any running server.
 */
class SeaTunnelContainerThreadExemptionTest {

    /** Reflected handle for the {@code private static isSystemThread(String)} method. */
    private static Method isSystemThreadMethod;

    /** Reflected handle for the {@code protected isIssueWeAlreadyKnow(String)} method. */
    private static Method isIssueWeAlreadyKnowMethod;

    /**
     * A Mockito mock of {@link SeaTunnelContainer} used solely to invoke the inherited {@code
     * protected} instance method {@code isIssueWeAlreadyKnow}. {@code CALLS_REAL_METHODS} ensures
     * the actual implementation is executed.
     */
    private static SeaTunnelContainer containerMock;

    @BeforeAll
    static void setUpReflection() throws Exception {
        isSystemThreadMethod =
                SeaTunnelContainer.class.getDeclaredMethod("isSystemThread", String.class);
        isSystemThreadMethod.setAccessible(true);

        isIssueWeAlreadyKnowMethod =
                SeaTunnelContainer.class.getDeclaredMethod("isIssueWeAlreadyKnow", String.class);
        isIssueWeAlreadyKnowMethod.setAccessible(true);

        // CALLS_REAL_METHODS delegates every unstubbed call to the real implementation.
        // The mock is never started (startUp() is never called) so no Docker activity occurs.
        containerMock = Mockito.mock(SeaTunnelContainer.class, Mockito.CALLS_REAL_METHODS);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static boolean isSystemThread(String name) throws Exception {
        return (boolean) isSystemThreadMethod.invoke(null, name);
    }

    private static boolean isIssueWeAlreadyKnow(String name) throws Exception {
        return (boolean) isIssueWeAlreadyKnowMethod.invoke(containerMock, name);
    }

    // -------------------------------------------------------------------------
    // Blocker 2a — Couchbase names must NOT be in the global isSystemThread whitelist
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "isSystemThread(\"{0}\") must be false")
    @ValueSource(
            strings = {
                "SimplePauseDetectorThread",
                "SimplePauseDetectorThread-1",
                "dnsjava NIO selector",
                "cb-cleaner",
                "cb-cleaner-1",
                "parallel-0",
                "parallel-1",
                "parallel-42"
            })
    void isSystemThread_couchbaseThreadNames_returnsFalse(String threadName) throws Exception {
        assertFalse(
                isSystemThread(threadName),
                "'"
                        + threadName
                        + "' must not be whitelisted globally in isSystemThread() — "
                        + "it should only be exempt via isIssueWeAlreadyKnow()");
    }

    // -------------------------------------------------------------------------
    // Blocker 2b — unique Couchbase thread names must always be exempt
    // -------------------------------------------------------------------------

    /**
     * The three names below are unique to the Couchbase SDK; no other connector produces them. They
     * must be exempt regardless of whether the SDK jar is on the classpath — the three startsWith
     * guards fire before the isCouchbaseSdkLoaded() gate.
     */
    @ParameterizedTest(name = "isIssueWeAlreadyKnow(\"{0}\") must be true")
    @ValueSource(
            strings = {
                "SimplePauseDetectorThread",
                "SimplePauseDetectorThread-1",
                "dnsjava NIO selector",
                "cb-cleaner",
                "cb-cleaner-1"
            })
    void isIssueWeAlreadyKnow_uniqueCouchbaseThreadNames_returnsTrue(String threadName)
            throws Exception {
        assertTrue(
                isIssueWeAlreadyKnow(threadName),
                "'"
                        + threadName
                        + "' must be recognised by isIssueWeAlreadyKnow() as a known "
                        + "Couchbase SDK singleton thread that survives Cluster.disconnect()");
    }

    // -------------------------------------------------------------------------
    // Blocker 2c — parallel-N exemption is gated on the Couchbase SDK being loaded
    // -------------------------------------------------------------------------

    /**
     * When the Couchbase SDK is NOT on the test classpath, {@code isCouchbaseSdkLoaded()} returns
     * {@code false} and a {@code parallel-N} thread must NOT be silently exempt. This is the core
     * scoping guarantee: a Reactor-based connector that leaks a {@code parallel-N} thread will be
     * caught in any E2E run where the Couchbase SDK was never loaded.
     *
     * <p>In this module ({@code seatunnel-e2e-common}) the Couchbase SDK is not a dependency, so
     * {@code Class.forName("com.couchbase.client.java.Cluster", false, ...) } throws {@code
     * ClassNotFoundException} and the guard returns {@code false}.
     */
    @Test
    void isIssueWeAlreadyKnow_parallelN_sdkNotLoaded_returnsFalse() throws Exception {
        boolean sdkPresent;
        try {
            Class.forName(
                    "com.couchbase.client.java.Cluster", false, ClassLoader.getSystemClassLoader());
            sdkPresent = true;
        } catch (ClassNotFoundException e) {
            sdkPresent = false;
        }
        org.junit.jupiter.api.Assumptions.assumeFalse(
                sdkPresent,
                "Couchbase SDK is on the classpath — SDK-not-loaded branch cannot be tested here");

        assertFalse(
                isIssueWeAlreadyKnow("parallel-0"),
                "parallel-0 must NOT be exempted when the Couchbase SDK is absent from the JVM");
        assertFalse(
                isIssueWeAlreadyKnow("parallel-42"),
                "parallel-42 must NOT be exempted when the Couchbase SDK is absent from the JVM");
    }

    /**
     * When the Couchbase SDK IS on the test classpath, {@code isCouchbaseSdkLoaded()} returns
     * {@code true} and a {@code parallel-N} thread IS correctly exempt. Skipped when the SDK is
     * absent (the module has no Couchbase dependency; this branch runs in the Couchbase E2E module
     * where the SDK is available).
     */
    @Test
    void isIssueWeAlreadyKnow_parallelN_sdkLoaded_returnsTrue() throws Exception {
        boolean sdkPresent;
        try {
            Class.forName(
                    "com.couchbase.client.java.Cluster", false, ClassLoader.getSystemClassLoader());
    // -------------------------------------------------------------------------
    // Sanity — non-numeric parallel thread names are never exempt
    // -------------------------------------------------------------------------

    /**
     * A non-numeric suffix must NOT match {@code parallel-\d+} regardless of whether the SDK is
     * loaded — "parallel-scheduler-1" from a different Reactor connector must still be reported.
     */
    @Test
    void isIssueWeAlreadyKnow_nonNumericParallelThread_returnsFalse() throws Exception {
        assertFalse(
                isIssueWeAlreadyKnow("parallel-scheduler-1"),
                "Thread 'parallel-scheduler-1' has a non-numeric suffix and must NOT match "
                        + "the Couchbase parallel-\\d+ pattern");
    }

    @Test
    void isIssueWeAlreadyKnow_parallelWithLetterSuffix_returnsFalse() throws Exception {
        assertFalse(
                isIssueWeAlreadyKnow("parallel-reactor"),
                "Thread 'parallel-reactor' must not match the Couchbase numeric-only pattern");
    }

    // -------------------------------------------------------------------------
    // Sanity — existing global system threads must still pass isSystemThread
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "isSystemThread(\"{0}\") must still be true")
    @ValueSource(
            strings = {
                "hz.main.something",
                "Log4j2-TF-1-ExecCtx",
                "commons-pool-evictor",
                "qtp123456-42",
                "heartbeat-checker"
            })
    void isSystemThread_existingGlobalThreads_stillReturnsTrue(String threadName) throws Exception {
        assertTrue(
                isSystemThread(threadName),
                "Pre-existing global system thread '" + threadName + "' must still be whitelisted");
    }
}
