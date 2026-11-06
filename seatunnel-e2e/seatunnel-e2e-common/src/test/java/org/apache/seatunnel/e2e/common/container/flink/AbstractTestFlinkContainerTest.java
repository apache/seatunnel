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

class AbstractTestFlinkContainerTest {

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

    /** Uses mocked containers and records the host cleanup instead of deleting a real path. */
    private static class TestFlinkContainer extends Flink18Container {

        static final String VOLUME = CONTAINER_VOLUME_MOUNT_PATH;

        boolean hostVolumeDeleted;

        TestFlinkContainer(GenericContainer<?> jobManager, GenericContainer<?> taskManager) {
            this.jobManager = jobManager;
            this.taskManager = taskManager;
        }

        @Override
        protected void deleteHostVolumeMountPath() {
            hostVolumeDeleted = true;
        }
    }
}
