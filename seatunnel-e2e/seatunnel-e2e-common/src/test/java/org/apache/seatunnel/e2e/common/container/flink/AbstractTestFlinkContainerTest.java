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
        Mockito.when(taskManager.execInContainer("rm", "-rf", TestFlinkContainer.VOLUME))
                .thenThrow(
                        new IllegalStateException(
                                "execInContainer can only be used while the Container is running"));
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(taskManager).stop();
        Mockito.verify(jobManager).execInContainer("rm", "-rf", TestFlinkContainer.VOLUME);
        Mockito.verify(jobManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldStopJobManagerWhenCleaningTaskManagerVolumeFails() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        IllegalStateException cleanupFailure = new IllegalStateException("exec failed");
        Mockito.when(taskManager.execInContainer("rm", "-rf", TestFlinkContainer.VOLUME))
                .thenThrow(cleanupFailure);
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        IllegalStateException thrown =
                Assertions.assertThrows(IllegalStateException.class, container::tearDown);

        Assertions.assertSame(cleanupFailure, thrown);
        Mockito.verify(taskManager).stop();
        Mockito.verify(jobManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldKeepFirstFailureAndSuppressLaterOnes() throws Exception {
        GenericContainer<?> jobManager = runningContainer();
        GenericContainer<?> taskManager = runningContainer();
        IllegalStateException cleanupFailure = new IllegalStateException("exec failed");
        IllegalStateException stopFailure = new IllegalStateException("stop failed");
        Mockito.when(taskManager.execInContainer("rm", "-rf", TestFlinkContainer.VOLUME))
                .thenThrow(cleanupFailure);
        Mockito.doThrow(stopFailure).when(jobManager).stop();
        TestFlinkContainer container = new TestFlinkContainer(jobManager, taskManager);

        IllegalStateException thrown =
                Assertions.assertThrows(IllegalStateException.class, container::tearDown);

        Assertions.assertSame(cleanupFailure, thrown);
        Assertions.assertArrayEquals(new Throwable[] {stopFailure}, thrown.getSuppressed());
        Mockito.verify(taskManager).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    private static GenericContainer<?> runningContainer() {
        GenericContainer<?> container = Mockito.mock(GenericContainer.class);
        Mockito.when(container.isRunning()).thenReturn(true);
        return container;
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
        void deleteHostVolumeMountPath() {
            hostVolumeDeleted = true;
        }
    }
}
