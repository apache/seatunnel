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

package org.apache.seatunnel.e2e.common.util;

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ContainerUtilTest {

    @Test
    void shouldCopyConnectorJarDirectlyWithoutContainerExec() throws Exception {
        GenericContainer<?> container = mock(GenericContainer.class);
        Path jar = Files.createTempFile("connector-direct-copy-", ".jar");
        try {
            ContainerUtil.copyConnectorJarDirectly(container, jar.toFile(), "/tmp/seatunnel/");

            verify(container)
                    .copyFileToContainer(
                            any(MountableFile.class),
                            eq("/tmp/seatunnel/connectors/" + jar.getFileName()));
            verify(container, never()).execInContainer(any(String[].class));
        } finally {
            Files.deleteIfExists(jar);
        }
    }

    @Test
    void shouldPublishNewContentUsingContainerPaths() throws Exception {
        try (ConnectorCopyFixture fixture = new ConnectorCopyFixture()) {
            fixture.copy();
            assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(fixture.published));
            assertEquals(
                    "/tmp/seatunnel/connectors/connector-fake-test.jar",
                    fixture.commands.get(0)[3]);
            fixture.assertNoStagedFiles();
        }
    }

    @Test
    void shouldPreserveThePublishedJarWhenCopyFails() throws Exception {
        try (ConnectorCopyFixture fixture = new ConnectorCopyFixture()) {
            fixture.failCopy = true;
            IllegalStateException failure =
                    assertThrows(IllegalStateException.class, fixture::copy);
            assertEquals("copy failed", failure.getMessage());
            assertEquals(1, fixture.commands.size());
            assertEquals("rm", fixture.commands.get(0)[0]);
            assertTrue(fixture.commands.get(0)[2].endsWith(".tmp"));
            fixture.assertOldContentAndNoStagedFiles();
        }
    }

    @Test
    void shouldRejectFailedPublicationAndCleanOnlyTheStagedFile() throws Exception {
        try (ConnectorCopyFixture fixture = new ConnectorCopyFixture()) {
            fixture.publishExit = 1;
            IllegalStateException failure =
                    assertThrows(IllegalStateException.class, fixture::copy);
            assertTrue(
                    failure.getMessage().contains("Could not publish connector JAR"),
                    failure.getMessage());
            assertTrue(failure.getMessage().contains("controlled failure"), failure.getMessage());
            assertEquals(2, fixture.commands.size());
            assertEquals("mv", fixture.commands.get(0)[0]);
            assertEquals("rm", fixture.commands.get(1)[0]);
            assertEquals(fixture.commands.get(0)[2], fixture.commands.get(1)[2]);
            assertTrue(fixture.commands.get(1)[2].endsWith(".tmp"));
            fixture.assertOldContentAndNoStagedFiles();
        }
    }

    @Test
    void shouldRestoreInterruptStatusAfterCleaningStagedFile() throws Exception {
        try (ConnectorCopyFixture fixture = new ConnectorCopyFixture()) {
            fixture.interruptPublish = true;
            try {
                IllegalStateException failure =
                        assertThrows(IllegalStateException.class, fixture::copy);
                assertTrue(
                        failure.getMessage().contains("Interrupted while publishing connector JAR"),
                        failure.getMessage());
                assertEquals("publication interrupted", failure.getCause().getMessage());
                assertTrue(Thread.currentThread().isInterrupted());
                assertEquals("rm", fixture.commands.get(1)[0]);
                fixture.assertOldContentAndNoStagedFiles();
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldKeepPublicationFailureWhenCleanupAlsoFails() throws Exception {
        try (ConnectorCopyFixture fixture = new ConnectorCopyFixture()) {
            fixture.publishExit = 1;
            fixture.cleanupExit = 1;
            IllegalStateException failure =
                    assertThrows(IllegalStateException.class, fixture::copy);
            assertTrue(failure.getMessage().contains("publish"), failure.getMessage());
            assertEquals(1, failure.getSuppressed().length);
            assertTrue(
                    failure.getSuppressed()[0]
                            .getMessage()
                            .contains("Could not remove staged connector JAR"));
            assertArrayEquals(new byte[] {9, 8, 7}, Files.readAllBytes(fixture.published));
        }
    }

    private static final class ConnectorCopyFixture implements AutoCloseable {
        private final GenericContainer<?> container = mock(GenericContainer.class);
        private final List<String[]> commands = new java.util.ArrayList<>();
        private final Path directory;
        private final Path jar;
        private final Path remote;
        private final Path published;
        private boolean failCopy;
        private int publishExit;
        private int cleanupExit;
        private boolean interruptPublish;

        private ConnectorCopyFixture() throws Exception {
            Path target = Paths.get("target").toAbsolutePath();
            Files.createDirectories(target);
            directory = Files.createTempDirectory(target, "connector-copy-");
            jar = directory.resolve("connector-fake-test.jar");
            Files.write(jar, new byte[] {1, 2, 3});
            remote = Files.createDirectory(directory.resolve("remote"));
            published = remote.resolve(jar.getFileName());
            Files.write(published, new byte[] {9, 8, 7});
            doAnswer(
                            invocation -> {
                                String targetPath = invocation.getArgument(1);
                                assertFalse(
                                        targetPath.contains("\\"),
                                        "Container paths must use POSIX separators: " + targetPath);
                                Path staged =
                                        remote.resolve(
                                                targetPath.substring(
                                                        targetPath.lastIndexOf('/') + 1));
                                Files.write(staged, new byte[] {1});
                                assertArrayEquals(
                                        new byte[] {9, 8, 7}, Files.readAllBytes(published));
                                if (failCopy) {
                                    throw new IllegalStateException("copy failed");
                                }
                                Files.copy(jar, staged, StandardCopyOption.REPLACE_EXISTING);
                                return null;
                            })
                    .when(container)
                    .copyFileToContainer(any(MountableFile.class), anyString());
            doAnswer(
                            invocation -> {
                                String[] command = (String[]) invocation.getRawArguments()[0];
                                commands.add(command);
                                if ("mv".equals(command[0]) && interruptPublish) {
                                    throw new InterruptedException("publication interrupted");
                                }
                                assertFalse(
                                        command[2].contains("\\"),
                                        "Container exec paths must use POSIX separators");
                                Path staged =
                                        remote.resolve(
                                                command[2].substring(
                                                        command[2].lastIndexOf('/') + 1));
                                if ("mv".equals(command[0]) && publishExit == 0) {
                                    Files.move(
                                            staged,
                                            published,
                                            StandardCopyOption.ATOMIC_MOVE,
                                            StandardCopyOption.REPLACE_EXISTING);
                                } else if ("rm".equals(command[0]) && cleanupExit == 0) {
                                    Files.deleteIfExists(staged);
                                }
                                Container.ExecResult result = mock(Container.ExecResult.class);
                                when(result.getExitCode())
                                        .thenReturn(
                                                "mv".equals(command[0])
                                                        ? publishExit
                                                        : cleanupExit);
                                when(result.getStdout()).thenReturn("");
                                when(result.getStderr()).thenReturn("controlled failure");
                                return result;
                            })
                    .when(container)
                    .execInContainer(any(String[].class));
        }

        private void copy() {
            ContainerUtil.copyConnectorJar(container, jar.toFile(), "/tmp/seatunnel");
        }

        private void assertOldContentAndNoStagedFiles() throws IOException {
            assertArrayEquals(new byte[] {9, 8, 7}, Files.readAllBytes(published));
            assertNoStagedFiles();
        }

        private void assertNoStagedFiles() throws IOException {
            try (java.util.stream.Stream<Path> files = Files.list(remote)) {
                assertEquals(
                        java.util.Collections.singletonList(published),
                        files.collect(Collectors.toList()));
            }
        }

        @Override
        public void close() throws IOException {
            try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
                for (Path path :
                        paths.sorted(java.util.Comparator.reverseOrder())
                                .collect(Collectors.toList())) {
                    Files.delete(path);
                }
            }
        }
    }

    private static final List<TestContainer> CONTAINERS =
            Arrays.asList(
                    container(TestContainerId.FLINK_1_18),
                    container(TestContainerId.FLINK_1_20),
                    container(TestContainerId.SPARK_3_3),
                    container(TestContainerId.SEATUNNEL));

    @Test
    void shouldKeepEveryContainerWhenRunEnginesIsUnset() {
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, null));
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, " "));
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, "[]"));
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, "all"));
    }

    @Test
    void shouldKeepOnlyTheContainersOfTheListedEngines() {
        assertEquals(
                Arrays.asList(TestContainerId.FLINK_1_18, TestContainerId.FLINK_1_20),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "flink")));
        assertEquals(
                Arrays.asList(TestContainerId.SPARK_3_3),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "'spark'")));
        assertEquals(
                Arrays.asList(
                        TestContainerId.FLINK_1_18,
                        TestContainerId.FLINK_1_20,
                        TestContainerId.SPARK_3_3),
                identifiers(
                        ContainerUtil.filterByRunEngines(CONTAINERS, "[\"flink\", \"spark\"]")));
    }

    @Test
    void shouldAcceptTheZetaAliases() {
        assertEquals(
                Arrays.asList(TestContainerId.SEATUNNEL),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "zeta")));
        assertEquals(
                Arrays.asList(TestContainerId.SEATUNNEL),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "SEATUNNEL")));
    }

    @Test
    void shouldKeepEveryContainerWhenNoDiscoveredContainerMatches() {
        List<TestContainer> zetaOnly = Arrays.asList(container(TestContainerId.SEATUNNEL));

        assertEquals(zetaOnly, ContainerUtil.filterByRunEngines(zetaOnly, "flink,spark"));
    }

    @Test
    void shouldFailFastForUnknownEngineNames() {
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ContainerUtil.filterByRunEngines(CONTAINERS, "zetta"));

        assertTrue(failure.getMessage().contains("zetta"), failure.getMessage());
    }

    private static List<TestContainerId> identifiers(List<TestContainer> containers) {
        return containers.stream().map(TestContainer::identifier).collect(Collectors.toList());
    }

    private static TestContainer container(TestContainerId identifier) {
        TestContainer container = mock(TestContainer.class);
        when(container.identifier()).thenReturn(identifier);
        return container;
    }
}
