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

package org.apache.seatunnel.e2e.common.container.flink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

class AbstractTestFlinkContainerTest {

    /** Generous host-side budget for the whole timeout path; the stubbed deadlines are 200 ms. */
    private static final Duration TEST_BUDGET = Duration.ofSeconds(10);

    /**
     * A TaskManager that failed to start must not stop the JobManager from being stopped. A leaked
     * JobManager keeps the {@code jobmanager} alias on the shared network, so the TaskManager of
     * the next test case can register with it and leave that case's job waiting for slots forever.
     */
    @Test
    void shouldStopJobManagerWhenTaskManagerIsNotRunning() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = Mockito.mock(GenericContainer.class);
        Mockito.when(taskManager.isRunning()).thenReturn(false);
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(taskManager, Mockito.never())
                .execInContainer("rm", "-rf", TestFlinkContainer.VOLUME);
        Mockito.verify(taskManager).stop();
        Mockito.verify(jobManager).execInContainer("rm", "-rf", TestFlinkContainer.VOLUME);
        Mockito.verify(jobManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    /**
     * The TaskManager can die between {@code isRunning()} and the exec, for example when it runs
     * out of metaspace. Both containers are still stopped, so the test case must not fail on the
     * best-effort volume cleanup.
     */
    @Test
    void shouldNotFailWhenTaskManagerStopsBeforeVolumeCleanup() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        Mockito.when(taskManager.execInContainer("rm", "-rf", TestFlinkContainer.VOLUME))
                .thenThrow(new IllegalStateException("container is not running"));
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(taskManager).stop();
        Mockito.verify(jobManager).execInContainer("rm", "-rf", TestFlinkContainer.VOLUME);
        Mockito.verify(jobManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    /** {@code rm -rf} exits with 1 on the bind-mount point itself, even after emptying it. */
    @Test
    void shouldNotFailWhenVolumeCleanupExitsWithError() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        Container.ExecResult failedResult = execResult(1);
        Mockito.when(taskManager.execInContainer("rm", "-rf", TestFlinkContainer.VOLUME))
                .thenReturn(failedResult);
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(taskManager).stop();
        Mockito.verify(jobManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldStopJobManagerAndRethrowWhenVolumeCleanupIsInterrupted() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        InterruptedException interrupted = new InterruptedException("interrupted");
        Mockito.when(taskManager.execInContainer("rm", "-rf", TestFlinkContainer.VOLUME))
                .thenThrow(interrupted);
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        InterruptedException thrown =
                Assertions.assertThrows(InterruptedException.class, container::tearDown);

        Assertions.assertSame(interrupted, thrown);
        Mockito.verify(taskManager).stop();
        Mockito.verify(jobManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldKeepFirstFailureAndSuppressLaterOnes() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        IllegalStateException taskManagerStopFailure = new IllegalStateException("stop failed");
        IllegalStateException jobManagerStopFailure = new IllegalStateException("stop failed");
        Mockito.doThrow(taskManagerStopFailure).when(taskManager).stop();
        Mockito.doThrow(jobManagerStopFailure).when(jobManager).stop();
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        IllegalStateException thrown =
                Assertions.assertThrows(IllegalStateException.class, container::tearDown);

        Assertions.assertSame(taskManagerStopFailure, thrown);
        Assertions.assertArrayEquals(
                new Throwable[] {jobManagerStopFailure}, thrown.getSuppressed());
        Mockito.verify(jobManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    /**
     * The job deadline and the thread dump deadline must both be enforced on the host. The job
     * never returns, the JobManager dump request stalls forever and the TaskManager dump request
     * fails, which is the worst case for a stalled Docker transport. The test fails when the job
     * deadline is removed, when the stalled dump is not bounded (the whole call would hang past the
     * test budget), when the TaskManager dump is skipped after the JobManager dump stalled, or when
     * a failing dump replaces the original timeout failure.
     */
    @Test
    void shouldFailAfterJobDeadlineAndBoundEachThreadDumpRequest() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        CountDownLatch never = new CountDownLatch(1);
        Mockito.when(
                        jobManager.execInContainer(
                                Mockito.eq("bash"), Mockito.eq("-c"), Mockito.anyString()))
                .thenAnswer(
                        invocation -> {
                            never.await();
                            return null;
                        });
        Mockito.when(
                        taskManager.execInContainer(
                                Mockito.eq("bash"), Mockito.eq("-c"), Mockito.anyString()))
                .thenThrow(new IOException("docker exec failed"));
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);
        container.jobSubmission =
                () -> {
                    never.await();
                    return null;
                };

        IllegalStateException thrown =
                Assertions.assertTimeoutPreemptively(
                        TEST_BUDGET,
                        () ->
                                Assertions.assertThrows(
                                        IllegalStateException.class,
                                        () -> container.executeJob("hang.conf")));

        Assertions.assertTrue(
                thrown.getCause() instanceof TimeoutException, String.valueOf(thrown.getCause()));
        Assertions.assertTrue(thrown.getMessage().contains("hang.conf"), thrown.getMessage());
        Mockito.verify(jobManager)
                .execInContainer(
                        Mockito.eq("bash"), Mockito.eq("-c"), Mockito.contains("kill -QUIT"));
        Mockito.verify(taskManager)
                .execInContainer(
                        Mockito.eq("bash"), Mockito.eq("-c"), Mockito.contains("kill -QUIT"));
    }

    /** A job that finishes in time returns its own result and triggers no diagnostics. */
    @Test
    void shouldReturnJobResultWithoutThreadDumpsWhenJobFinishesInTime() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);
        Container.ExecResult result = execResult(0);
        container.jobSubmission = () -> result;

        Assertions.assertSame(result, container.executeJob("finishes.conf"));

        Mockito.verify(jobManager, Mockito.never())
                .execInContainer(Mockito.eq("bash"), Mockito.eq("-c"), Mockito.anyString());
        Mockito.verify(taskManager, Mockito.never())
                .execInContainer(Mockito.eq("bash"), Mockito.eq("-c"), Mockito.anyString());
    }

    /**
     * Running the submission on another thread must not change what callers see: the original
     * exception instance is rethrown, not an ExecutionException or a wrapper around it.
     */
    @Test
    void shouldRethrowJobFailureUnchanged() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);
        IOException failure = new IOException("submission failed");
        container.jobSubmission =
                () -> {
                    throw failure;
                };

        IOException thrown =
                Assertions.assertThrows(
                        IOException.class, () -> container.executeJob("fails.conf"));

        Assertions.assertSame(failure, thrown);
        Mockito.verify(jobManager, Mockito.never())
                .execInContainer(Mockito.eq("bash"), Mockito.eq("-c"), Mockito.anyString());
    }

    private static GenericContainer<?> runningContainer() throws Exception {
        GenericContainer<?> container = Mockito.mock(GenericContainer.class);
        Mockito.when(container.isRunning()).thenReturn(true);
        Container.ExecResult result = execResult(0);
        Mockito.when(container.execInContainer("rm", "-rf", TestFlinkContainer.VOLUME))
                .thenReturn(result);
        return container;
    }

    private static Container.ExecResult execResult(int exitCode) {
        Container.ExecResult result = Mockito.mock(Container.ExecResult.class);
        Mockito.when(result.getExitCode()).thenReturn(exitCode);
        return result;
    }

    /** Stands in for the docker exec that submits a job, so a test can finish, fail or hang it. */
    private interface JobSubmission {
        Container.ExecResult submit() throws IOException, InterruptedException;
    }

    /**
     * Uses mocked containers and records the host cleanup instead of deleting a real path. The job
     * and thread dump deadlines are shortened so the timeout path runs within a unit test.
     */
    private static class TestFlinkContainer extends Flink18Container {

        static final String VOLUME = CONTAINER_VOLUME_MOUNT_PATH;

        boolean hostVolumeDeleted;

        JobSubmission jobSubmission;

        TestFlinkContainer(GenericContainer<?> jobManager, GenericContainer<?> taskManager) {
            this.jobManager = jobManager;
            this.taskManager = taskManager;
        }

        @Override
        protected void deleteHostVolumeMountPath() {
            hostVolumeDeleted = true;
        }

        @Override
        protected Container.ExecResult executeJob(
                GenericContainer<?> container,
                String confFile,
                String jobId,
                List<String> variables)
                throws IOException, InterruptedException {
            return jobSubmission.submit();
        }

        @Override
        Duration getJobTimeout() {
            return Duration.ofMillis(200);
        }

        @Override
        Duration getThreadDumpTimeout() {
            return Duration.ofMillis(200);
        }
    }
}
