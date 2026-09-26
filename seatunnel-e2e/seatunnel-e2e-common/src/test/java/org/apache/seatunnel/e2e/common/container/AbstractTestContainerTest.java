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

package org.apache.seatunnel.e2e.common.container;

import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.container.spark.Spark3Container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

class AbstractTestContainerTest {

    private static final String VOLUME = AbstractTestContainer.CONTAINER_VOLUME_MOUNT_PATH;

    @Test
    void shouldSkipVolumeCleanupWhenContainerIsNotRunning() throws Exception {
        GenericContainer<?> container = stoppedContainer();

        Assertions.assertFalse(AbstractTestContainer.removeContainerVolumeMountPath(container));

        Mockito.verify(container, Mockito.never()).execInContainer("rm", "-rf", VOLUME);
    }

    /** The container can stop between {@code isRunning()} and the exec. */
    @Test
    void shouldNotThrowWhenVolumeCleanupFails() throws Exception {
        GenericContainer<?> container = runningContainer(0);
        Mockito.when(container.execInContainer("rm", "-rf", VOLUME))
                .thenThrow(new IllegalStateException("container is not running"));

        Assertions.assertFalse(AbstractTestContainer.removeContainerVolumeMountPath(container));
    }

    /**
     * {@code rm -rf} exits with 1 on the bind-mount point itself even after removing its contents,
     * so the exit code does not mean the cleanup failed.
     */
    @Test
    void shouldRunVolumeCleanupOnRunningContainer() throws Exception {
        GenericContainer<?> container = runningContainer(1);

        Assertions.assertTrue(AbstractTestContainer.removeContainerVolumeMountPath(container));

        Mockito.verify(container).execInContainer("rm", "-rf", VOLUME);
    }

    @Test
    void shouldReportHostPathThatCannotBeDeleted(@TempDir Path tempDir) throws Exception {
        File volume = tempDir.resolve("volume").toFile();
        File readOnlyDir = new File(volume, "written-by-container");
        Assertions.assertTrue(readOnlyDir.mkdirs());
        Files.write(new File(readOnlyDir, "part-0").toPath(), new byte[] {1});
        Assertions.assertTrue(readOnlyDir.setWritable(false));
        try {
            Assumptions.assumeFalse(readOnlyDir.canWrite(), "running as a user that ignores mode");

            Assertions.assertFalse(AbstractTestContainer.deleteHostPath(volume.getPath()));
            Assertions.assertTrue(volume.exists());
        } finally {
            readOnlyDir.setWritable(true);
        }
        Assertions.assertTrue(AbstractTestContainer.deleteHostPath(volume.getPath()));
        Assertions.assertFalse(volume.exists());
    }

    @Test
    void shouldStopSparkMasterWhenItIsNotRunning() throws Exception {
        GenericContainer<?> master = stoppedContainer();
        TestSparkContainer container = new TestSparkContainer(master);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(master, Mockito.never()).execInContainer("rm", "-rf", VOLUME);
        Mockito.verify(master).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldStopSparkMasterWhenItStopsBeforeVolumeCleanup() throws Exception {
        GenericContainer<?> master = runningContainer(0);
        Mockito.when(master.execInContainer("rm", "-rf", VOLUME))
                .thenThrow(new IllegalStateException("container is not running"));
        TestSparkContainer container = new TestSparkContainer(master);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(master).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldCleanVolumeAndStopRunningSparkMaster() throws Exception {
        GenericContainer<?> master = runningContainer(0);
        TestSparkContainer container = new TestSparkContainer(master);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(master).execInContainer("rm", "-rf", VOLUME);
        Mockito.verify(master).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldStopSeaTunnelServerWhenItIsNotRunning() throws Exception {
        GenericContainer<?> server = stoppedContainer();
        TestSeaTunnelContainer container = new TestSeaTunnelContainer(server);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(server, Mockito.never()).execInContainer("rm", "-rf", VOLUME);
        Mockito.verify(server).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldStopSeaTunnelServerWhenItStopsBeforeVolumeCleanup() throws Exception {
        GenericContainer<?> server = runningContainer(0);
        Mockito.when(server.execInContainer("rm", "-rf", VOLUME))
                .thenThrow(new IllegalStateException("container is not running"));
        TestSeaTunnelContainer container = new TestSeaTunnelContainer(server);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(server).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldCleanVolumeAndStopRunningSeaTunnelServer() throws Exception {
        GenericContainer<?> server = runningContainer(0);
        TestSeaTunnelContainer container = new TestSeaTunnelContainer(server);

        Assertions.assertDoesNotThrow(container::tearDown);

        Mockito.verify(server).execInContainer("rm", "-rf", VOLUME);
        Mockito.verify(server).stop();
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    @Test
    void shouldRethrowSeaTunnelServerStopFailure() {
        GenericContainer<?> server = stoppedContainer();
        IllegalStateException stopFailure = new IllegalStateException("stop failed");
        Mockito.doThrow(stopFailure).when(server).stop();
        TestSeaTunnelContainer container = new TestSeaTunnelContainer(server);

        IllegalStateException thrown =
                Assertions.assertThrows(IllegalStateException.class, container::tearDown);

        Assertions.assertSame(stopFailure, thrown);
        Assertions.assertTrue(container.hostVolumeDeleted);
    }

    private static GenericContainer<?> stoppedContainer() {
        GenericContainer<?> container = Mockito.mock(GenericContainer.class);
        Mockito.when(container.isRunning()).thenReturn(false);
        return container;
    }

    private static GenericContainer<?> runningContainer(int cleanupExitCode) throws Exception {
        GenericContainer<?> container = Mockito.mock(GenericContainer.class);
        Mockito.when(container.isRunning()).thenReturn(true);
        Container.ExecResult result = Mockito.mock(Container.ExecResult.class);
        Mockito.when(result.getExitCode()).thenReturn(cleanupExitCode);
        Mockito.when(container.execInContainer("rm", "-rf", VOLUME)).thenReturn(result);
        return container;
    }

    /** Uses a mocked master and records the host cleanup instead of deleting a real path. */
    private static class TestSparkContainer extends Spark3Container {

        boolean hostVolumeDeleted;

        TestSparkContainer(GenericContainer<?> master) {
            this.master = master;
        }

        @Override
        protected void deleteHostVolumeMountPath() {
            hostVolumeDeleted = true;
        }
    }

    /** Uses a mocked server and records the host cleanup instead of deleting a real path. */
    private static class TestSeaTunnelContainer extends SeaTunnelContainer {

        boolean hostVolumeDeleted;

        TestSeaTunnelContainer(GenericContainer<?> server) {
            this.server = server;
        }

        @Override
        protected void deleteHostVolumeMountPath() {
            hostVolumeDeleted = true;
        }
    }
}
