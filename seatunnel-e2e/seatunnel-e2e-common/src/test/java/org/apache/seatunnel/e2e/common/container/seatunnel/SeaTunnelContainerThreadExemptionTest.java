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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for lifecycle-scoped third-party client thread exemptions.
 *
 * <p>Before the fix, four Couchbase-specific thread names were exempted inside the <em>global</em>
 * {@code isSystemThread()} method. This meant any Reactor-based connector that leaked a scheduler
 * thread matching {@code parallel-\d+} would silently pass the E2E thread-leak check.
 *
 * <p>After the fix:
 *
 * <ul>
 *   <li>{@code isSystemThread()} must return {@code false} for all four Couchbase thread names.
 *   <li>The three uniquely-named threads are exempt via {@code isIssueWeAlreadyKnow()}.
 *   <li>{@code parallel-N} is exempt via {@code isIssueWeAlreadyKnow()} only while the Couchbase
 *       E2E lifecycle flag ({@link SeaTunnelContainer#couchbaseE2eActive}) is {@code true},
 *       preventing false exemptions from other Reactor connectors.
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

    /**
     * Resets lifecycle flags after every test that may have set one, so tests remain isolated
     * regardless of execution order.
     */
    @AfterEach
    void resetLifecycleFlags() {
        SeaTunnelContainer.disableCouchbaseParallelThreadExemption();
        SeaTunnelContainer.disableGcsOpenCensusThreadExemption();
    }

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
     * must be exempt regardless of whether {@link SeaTunnelContainer#couchbaseE2eActive} is set —
     * the three {@code startsWith} guards fire unconditionally, before the lifecycle-flag check.
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
    // Blocker 2c — parallel-N exemption is scoped to the Couchbase E2E lifecycle flag
    // -------------------------------------------------------------------------

    /**
     * When the Couchbase E2E lifecycle flag is {@code false} (the default), a {@code parallel-N}
     * thread must NOT be silently exempt. This is the core scoping guarantee: a Reactor-based
     * connector that leaks a {@code parallel-N} thread will be caught in any E2E run where the
     * Couchbase test has not set the flag — regardless of whether the Couchbase SDK jar happens to
     * be on the classpath.
     */
    @Test
    void isIssueWeAlreadyKnow_parallelN_flagOff_returnsFalse() throws Exception {
        // Default state — flag is off. The @AfterEach ensures it stays off between tests.
        assertFalse(
                isIssueWeAlreadyKnow("parallel-0"),
                "parallel-0 must NOT be exempted when the Couchbase E2E flag is off");
        assertFalse(
                isIssueWeAlreadyKnow("parallel-42"),
                "parallel-42 must NOT be exempted when the Couchbase E2E flag is off");
    }

    /**
     * When {@link SeaTunnelContainer#enableCouchbaseParallelThreadExemption()} has been called (as
     * {@code CouchbaseIT.startUp()} does), a {@code parallel-N} thread IS correctly exempt.
     *
     * <p>The {@code @AfterEach} hook calls {@link
     * SeaTunnelContainer#disableCouchbaseParallelThreadExemption()} to reset the flag after this
     * test completes.
     */
    @Test
    void isIssueWeAlreadyKnow_parallelN_flagOn_returnsTrue() throws Exception {
        SeaTunnelContainer.enableCouchbaseParallelThreadExemption();

        assertTrue(
                isIssueWeAlreadyKnow("parallel-0"),
                "parallel-0 must be exempted when the Couchbase E2E flag is on");
        assertTrue(
                isIssueWeAlreadyKnow("parallel-42"),
                "parallel-42 must be exempted when the Couchbase E2E flag is on");
    }

    // -------------------------------------------------------------------------
    // Sanity — non-numeric parallel thread names are never exempt
    // -------------------------------------------------------------------------

    /**
     * A non-numeric suffix must NOT match {@code parallel-\d+} regardless of whether the lifecycle
     * flag is set — "parallel-scheduler-1" from a different Reactor connector must still be
     * reported.
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

    @ParameterizedTest(name = "GCS OpenCensus thread {0} is scoped to the GCS E2E lifecycle")
    @ValueSource(strings = {"ExportComponent.ServiceExporterThread-0", "OpenCensus.Disruptor-0"})
    void isIssueWeAlreadyKnow_gcsOpenCensusThreads_areLifecycleScoped(String threadName)
            throws Exception {
        assertFalse(
                isIssueWeAlreadyKnow(threadName),
                "GCS OpenCensus threads must not be exempted outside the GCS E2E lifecycle");

        SeaTunnelContainer.enableGcsOpenCensusThreadExemption();

        assertTrue(
                isIssueWeAlreadyKnow(threadName),
                "GCS OpenCensus threads must be exempted while the GCS E2E is active");
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
