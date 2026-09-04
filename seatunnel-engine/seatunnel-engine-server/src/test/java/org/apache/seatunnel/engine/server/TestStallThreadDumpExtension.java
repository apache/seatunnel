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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Logs a full JVM thread dump when a test class's bootstrap stalls, so hangs like the recurring
 * Windows-only {@code IllegalStateException: Node failed to start!} become diagnosable from CI
 * logs.
 *
 * <p>That failure surfaces after ~311 seconds (Hazelcast's 300-second default {@code
 * hazelcast.max.join.seconds} plus shutdown overhead) with no engine log line between the config
 * load and the exception, so the blocked frame never reaches CI output and each occurrence has been
 * untraceable. The stall always happens inside {@code @BeforeAll} while {@code
 * SeaTunnelServerStarter.createHazelcastInstance} boots the local member, which is why this
 * extension only monitors the window between the {@code beforeAll} callback and the first {@code
 * beforeEach} (or {@code afterAll} for classes whose tests never run): legitimate long-running test
 * methods must not trigger spurious dumps.
 *
 * <p>The dump threshold is intentionally below Hazelcast's 300-second join ceiling so the dump is
 * taken while the startup thread is still parked in the blocked frame, not after the node has
 * already given up and torn itself down.
 *
 * <p>Registered via {@code META-INF/services/org.junit.jupiter.api.extension.Extension} with {@code
 * junit.jupiter.extensions.autodetection.enabled=true} rather than a launcher {@code
 * TestExecutionListener}, because the module's test classpath only provides junit-jupiter-api and
 * adding junit-platform-launcher would require a pom change.
 */
@Slf4j
public class TestStallThreadDumpExtension
        implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback {

    /** Bootstrap time budget after which a stall is assumed and a thread dump is logged. */
    private static final long STALL_THRESHOLD_MILLIS = TimeUnit.SECONDS.toMillis(240);

    /** How often the watchdog thread re-checks the tracked bootstrap windows. */
    private static final long SCAN_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(30);

    /**
     * Test classes currently inside their bootstrap window, mapped to the wall-clock start
     * timestamp of that window.
     */
    private static final Map<String, Long> BOOTSTRAP_START_MILLIS = new ConcurrentHashMap<>();

    /** Classes already dumped once, so a stalled class does not flood the log every scan. */
    private static final Set<String> ALREADY_DUMPED = ConcurrentHashMap.newKeySet();

    /** Ensures the single watchdog daemon thread is started at most once per JVM. */
    private static final AtomicBoolean WATCHDOG_STARTED = new AtomicBoolean(false);

    @Override
    public void beforeAll(ExtensionContext context) {
        // Extension beforeAll callbacks run before the class's own @BeforeAll methods, so this
        // timestamp covers the whole server-bootstrap window where the known stalls occur.
        BOOTSTRAP_START_MILLIS.put(context.getRequiredTestClass().getName(), currentTimeMillis());
        startWatchdogIfNeeded();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        // The first test-method setup proves @BeforeAll completed; stop monitoring this class so
        // long-running test methods cannot trigger a spurious dump.
        BOOTSTRAP_START_MILLIS.remove(context.getRequiredTestClass().getName());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        // Covers classes whose test methods never start (all skipped, or bootstrap threw).
        BOOTSTRAP_START_MILLIS.remove(context.getRequiredTestClass().getName());
    }

    private static void startWatchdogIfNeeded() {
        if (!WATCHDOG_STARTED.compareAndSet(false, true)) {
            return;
        }
        Thread watchdog =
                new Thread(TestStallThreadDumpExtension::scanLoop, "seatunnel-test-stall-watchdog");
        // A daemon thread must never keep the surefire JVM alive after the test run finishes.
        watchdog.setDaemon(true);
        watchdog.start();
    }

    private static void scanLoop() {
        while (true) {
            try {
                Thread.sleep(SCAN_PERIOD_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            long now = currentTimeMillis();
            for (Map.Entry<String, Long> entry : BOOTSTRAP_START_MILLIS.entrySet()) {
                long elapsedMillis = now - entry.getValue();
                if (elapsedMillis >= STALL_THRESHOLD_MILLIS && ALREADY_DUMPED.add(entry.getKey())) {
                    log.error(
                            "Test class {} has been inside its bootstrap window for {} seconds;"
                                    + " logging a full thread dump before the Hazelcast join"
                                    + " timeout converts this stall into an opaque"
                                    + " 'Node failed to start!' failure.\n{}",
                            entry.getKey(),
                            TimeUnit.MILLISECONDS.toSeconds(elapsedMillis),
                            fullThreadDump());
                }
            }
        }
    }

    /**
     * Renders every live thread with its complete stack.
     *
     * <p>{@link ThreadInfo#toString()} silently truncates stacks to 8 frames, which is useless for
     * locating a frame parked deep inside Hazelcast bootstrap, so the frames are formatted
     * manually.
     */
    private static String fullThreadDump() {
        ThreadInfo[] threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
        StringBuilder dump = new StringBuilder(threads.length * 512);
        for (ThreadInfo thread : threads) {
            if (thread == null) {
                continue;
            }
            dump.append('"')
                    .append(thread.getThreadName())
                    .append("\" id=")
                    .append(thread.getThreadId())
                    .append(' ')
                    .append(thread.getThreadState());
            if (thread.getLockName() != null) {
                dump.append(" on ").append(thread.getLockName());
            }
            if (thread.getLockOwnerName() != null) {
                dump.append(" owned by \"")
                        .append(thread.getLockOwnerName())
                        .append("\" id=")
                        .append(thread.getLockOwnerId());
            }
            dump.append('\n');
            for (StackTraceElement frame : thread.getStackTrace()) {
                dump.append("    at ").append(frame).append('\n');
            }
            for (MonitorInfo monitor : thread.getLockedMonitors()) {
                dump.append("    locked ")
                        .append(monitor)
                        .append(" at frame depth ")
                        .append(monitor.getLockedStackDepth())
                        .append('\n');
            }
            dump.append('\n');
        }
        return dump.toString();
    }

    private static long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
