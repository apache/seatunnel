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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.engine.common.job.JobStatus;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@Slf4j
public class JobClientJobProxyIT extends SeaTunnelEngineContainer {

    /**
     * Single deadline for the asynchronous savepoint/restore lifecycle of the save-mode test. Every
     * submitted CLI command and every status poll is bounded by what is left of it, so a job or
     * command that never completes fails the test instead of hanging until the CI timeout.
     */
    private static final Duration SAVE_MODE_TEST_TIMEOUT = Duration.ofMinutes(6);

    /** Bound for stopping a leaked job and settling its command tasks during test cleanup. */
    private static final Duration SAVE_MODE_CLEANUP_TIMEOUT = Duration.ofSeconds(30);

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        // use seatunnel_fixed_slot_num.yaml replace seatunnel.yaml in container
        this.server =
                createSeaTunnelContainerWithFakeSourceAndInMemorySink(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel_fixed_slot_num.yaml");
    }

    @Test
    public void testJobRetryTimes() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/retry-times/stream_fake_to_inmemory_with_error_retry_1.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                server.getLogs()
                        .contains(
                                "Restore time 1, pipeline Job stream_fake_to_inmemory_with_error_retry_1.conf"));
        Assertions.assertFalse(
                server.getLogs()
                        .contains(
                                "Restore time 3, pipeline Job stream_fake_to_inmemory_with_error_retry_1.conf"));

        Container.ExecResult execResult2 =
                executeJob(server, "/retry-times/stream_fake_to_inmemory_with_error.conf");
        Assertions.assertNotEquals(0, execResult2.getExitCode());
        Assertions.assertTrue(
                server.getLogs()
                        .contains(
                                "Restore time 3, pipeline Job stream_fake_to_inmemory_with_error.conf"),
                server.getLogs());
    }

    @Test
    public void testNoDuplicatedReleaseSlot() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/savemode/fake_to_inmemory_savemode.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertFalse(
                server.getLogs().contains("wrong target release operation with job"));
    }

    @Test
    public void testNoExceptionLogWhenCancelJob() throws IOException, InterruptedException {
        String jobId = String.valueOf(System.currentTimeMillis());
        CompletableFuture.runAsync(
                () -> {
                    try {
                        executeJob(
                                "/stream_fakesource_to_inmemory_pending_row_in_queue.conf", jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException();
                    }
                });

        given().await()
                .pollDelay(5, TimeUnit.SECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .pollDelay(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals("RUNNING", this.getJobStatus(jobId));
                        });

        String logBeforeCancel = this.getServerLogs();
        cancelJob(jobId);
        given().pollDelay(10, TimeUnit.SECONDS)
                .await()
                .pollDelay(5000L, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals("CANCELED", this.getJobStatus(jobId));
                        });
        String logAfterCancel = this.getServerLogs().substring(logBeforeCancel.length());
        // in TaskExecutionService.BlockingWorker::run catch Throwable
        Assertions.assertFalse(logAfterCancel.contains("Exception in"), logAfterCancel);
        Assertions.assertEquals(
                4, StringUtils.countMatches(logAfterCancel, "Interrupted task"), logAfterCancel);
    }

    @Test
    public void testMultiTableSinkFailedWithThrowable() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/stream_fake_to_inmemory_with_throwable_error.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                execResult.getStderr().contains("table fake sink throw error"),
                execResult.getStderr());
    }

    @Test
    public void testSaveModeOnMasterOrClient() throws Throwable {
        // One deadline for the whole savepoint/restore lifecycle below: the surrounding batch jobs
        // are finite and bounded by docker exec, while every asynchronous command and status poll
        // of the streaming part must fit in what is left of this budget.
        Instant deadline = Instant.now().plus(SAVE_MODE_TEST_TIMEOUT);

        Container.ExecResult execResult =
                executeJob(server, "/savemode/fake_to_inmemory_savemode.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        int serverLogLength = 0;
        String serverLogs = server.getLogs();
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle schema savemode with table path: test.table1"));
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle data savemode with table path: test.table1"));
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle schema savemode with table path: test.table2"));
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle data savemode with table path: test.table2"));

        // restore will not execute savemode. A real savepoint is required since the client now
        // rejects a restore whose source job has no restore-eligible checkpoint data, so this
        // submits a streaming job with checkpointing enabled, stops it with a savepoint and
        // restores that savepoint. Every command runs as a bounded task on commandExecutor so a
        // failing savepoint/restore fails the test instead of blocking it, and the finally block
        // stops whatever is still running and settles those tasks.
        String streamingConf = "/savemode/stream_fake_to_inmemory_savemode.conf";
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        ExecutorService commandExecutor = newCommandExecutor();
        Map<Future<?>, String> commands = new LinkedHashMap<>();
        Throwable primaryFailure = null;
        try {
            Future<Container.ExecResult> sourceJobCommand =
                    submitCommand(
                            commandExecutor,
                            commands,
                            "savepoint source job " + jobId,
                            () -> executeJob(streamingConf, jobId));
            awaitJobStatus(jobId, JobStatus.RUNNING, deadline);

            Future<Container.ExecResult> savepointCommand =
                    submitCommand(
                            commandExecutor,
                            commands,
                            "savepoint of job " + jobId,
                            () -> savepointJob(jobId));
            Container.ExecResult savepointResult =
                    awaitCommand(savepointCommand, deadline, "savepoint of job " + jobId);
            Assertions.assertEquals(0, savepointResult.getExitCode(), savepointResult.getStderr());
            awaitJobStatus(jobId, JobStatus.SAVEPOINT_DONE, deadline);
            Container.ExecResult sourceJobResult =
                    awaitCommand(sourceJobCommand, deadline, "savepoint source job " + jobId);
            Assertions.assertEquals(0, sourceJobResult.getExitCode(), sourceJobResult.getStderr());

            // clear old logs
            serverLogs = server.getLogs();
            serverLogLength = serverLogs.length();

            Future<Container.ExecResult> restoreCommand =
                    submitCommand(
                            commandExecutor,
                            commands,
                            "restore job command " + jobId,
                            () -> restoreJob(streamingConf, jobId));
            awaitJobStatus(jobId, JobStatus.RUNNING, deadline);
            serverLogs = server.getLogs().substring(serverLogLength);
            Assertions.assertFalse(serverLogs.contains("handle schema savemode with table path"));
            Assertions.assertFalse(serverLogs.contains("handle data savemode with table path"));

            Future<?> stopCommand =
                    submitCommand(
                            commandExecutor,
                            commands,
                            "stop of restored job " + jobId,
                            () -> {
                                stopJob(jobId);
                                return null;
                            });
            awaitCommand(stopCommand, deadline, "stop of restored job " + jobId);
            awaitJobStatus(jobId, JobStatus.CANCELED, deadline);
            execResult = awaitCommand(restoreCommand, deadline, "restore job command " + jobId);
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            serverLogs = server.getLogs().substring(serverLogLength);
        } catch (Throwable failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            Exception cleanupFailure = null;
            try {
                cleanupFailure = stopJobAndSettleCommands(commandExecutor, jobId, commands);
            } finally {
                // Shut the executor down on its own, so a cleanup that throws or hangs on a
                // bounded await can never keep the command threads (and the JVM) alive.
                commandExecutor.shutdownNow();
            }
            if (cleanupFailure != null) {
                if (primaryFailure != null) {
                    // Preserve the primary failure; report the cleanup problem beside it.
                    primaryFailure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            }
        }

        // test savemode on client side
        Container.ExecResult execResult2 =
                executeJob(server, "/savemode/fake_to_inmemory_savemode_client.conf");
        Assertions.assertEquals(0, execResult2.getExitCode(), execResult2.getStderr());
        // clear old logs
        serverLogLength += serverLogs.length();
        serverLogs = server.getLogs().substring(serverLogLength);
        Assertions.assertFalse(serverLogs.contains("handle schema savemode with table path"));
        Assertions.assertFalse(serverLogs.contains("handle data savemode with table path"));

        Assertions.assertTrue(
                execResult2.getStdout().contains("handle schema savemode with table path"));
        Assertions.assertTrue(
                execResult2.getStdout().contains("handle data savemode with table path"));
    }

    @Test
    public void testJobFailedWillThrowException() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/batch_slot_not_enough.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                StringUtils.isNotBlank(execResult.getStderr())
                        && execResult
                                .getStderr()
                                .contains(
                                        "org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException"));
    }

    /** Creates a daemon-thread executor so a stuck CLI task can never keep the JVM alive. */
    private static ExecutorService newCommandExecutor() {
        AtomicInteger commandIndex = new AtomicInteger();
        return Executors.newCachedThreadPool(
                runnable -> {
                    Thread thread =
                            new Thread(
                                    runnable,
                                    "save-mode-it-command-" + commandIndex.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
    }

    private static <T> Future<T> submitCommand(
            ExecutorService commandExecutor,
            Map<Future<?>, String> commands,
            String description,
            Callable<T> command) {
        Future<T> future = commandExecutor.submit(command);
        commands.put(future, description);
        return future;
    }

    /**
     * Waits for a command result within the remaining test budget.
     *
     * <p>Never waits past the deadline: on timeout the command is cancelled (interrupting the
     * blocked docker exec where possible) and the wait fails with the command description.
     */
    private static <T> T awaitCommand(Future<T> command, Instant deadline, String description)
            throws Exception {
        try {
            return command.get(remainingMillis(deadline, description), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            command.cancel(true);
            TimeoutException deadlineExceeded =
                    new TimeoutException(
                            "Timed out after "
                                    + SAVE_MODE_TEST_TIMEOUT
                                    + " waiting for "
                                    + description);
            deadlineExceeded.initCause(e);
            throw deadlineExceeded;
        }
    }

    /** Returns the patience left until the deadline, failing instead of waiting past it. */
    private static long remainingMillis(Instant deadline, String description) {
        long remaining = Duration.between(Instant.now(), deadline).toMillis();
        if (remaining <= 0) {
            throw new IllegalStateException(
                    "Timed out after "
                            + SAVE_MODE_TEST_TIMEOUT
                            + " waiting for "
                            + description
                            + " (overall test deadline)");
        }
        return remaining;
    }

    private void awaitJobStatus(String jobId, JobStatus expectedStatus, Instant deadline) {
        Awaitility.await()
                .atMost(
                        remainingMillis(deadline, "job " + jobId + " to reach " + expectedStatus),
                        TimeUnit.MILLISECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus.name(), getJobStatus(jobId), jobId));
    }

    /**
     * Cleanup for the asynchronous savepoint/restore lifecycle of the save-mode test.
     *
     * <p>Reads the job status and, when the test left the job running (the source job before its
     * savepoint, or the restored job before its cancel), stops it, then settles every submitted
     * command task. Every step is bounded: the status read is submitted to the same command
     * executor and awaited with {@link #SAVE_MODE_CLEANUP_TIMEOUT} left, so a stalled REST response
     * fails this cleanup instead of blocking the caller's finally block. Returns the cleanup
     * failure instead of throwing it so the caller can attach it to the primary failure as a
     * suppressed exception.
     */
    private Exception stopJobAndSettleCommands(
            ExecutorService commandExecutor, String jobId, Map<Future<?>, String> commands) {
        Exception failure = null;
        try {
            String statusDescription = "cleanup status of job " + jobId;
            Future<String> statusCommand =
                    submitCommand(
                            commandExecutor,
                            commands,
                            statusDescription,
                            () -> getJobStatus(jobId));
            String status =
                    awaitCommand(
                            statusCommand,
                            Instant.now().plus(SAVE_MODE_CLEANUP_TIMEOUT),
                            statusDescription);
            if (status != null && !JobStatus.fromString(status).isEndState()) {
                Future<?> stopCommand =
                        submitCommand(
                                commandExecutor,
                                commands,
                                "cleanup stop of job " + jobId,
                                () -> {
                                    stopJob(jobId);
                                    return null;
                                });
                awaitCommand(
                        stopCommand,
                        Instant.now().plus(SAVE_MODE_CLEANUP_TIMEOUT),
                        "cleanup stop of job " + jobId);
            }
        } catch (Exception e) {
            failure = e;
        }

        for (Map.Entry<Future<?>, String> command : commands.entrySet()) {
            try {
                command.getKey().get(SAVE_MODE_CLEANUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (CancellationException e) {
                // Already cancelled: there is nothing left to settle.
            } catch (ExecutionException e) {
                // A command failure is asserted where its result is used; it only adds context
                // here and must not hide the primary failure.
                failure = appendSuppressed(failure, e);
            } catch (TimeoutException e) {
                command.getKey().cancel(true);
                failure =
                        appendSuppressed(
                                failure,
                                new TimeoutException(
                                        command.getValue()
                                                + " did not settle within "
                                                + SAVE_MODE_CLEANUP_TIMEOUT));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                command.getKey().cancel(true);
                failure = appendSuppressed(failure, e);
            }
        }
        return failure;
    }

    private static Exception appendSuppressed(Exception primary, Exception next) {
        if (primary == null) {
            return next;
        }
        primary.addSuppressed(next);
        return primary;
    }
}
