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

package org.apache.seatunnel.resource.core.classloader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApplicationJarPathResolverTest {
    @TempDir private Path directory;

    @Test
    void resolvesJarWithinRelocatedDistribution() throws Exception {
        Path master = directory.resolve("master-container/seatunnel");
        Path worker = directory.resolve("worker-container/seatunnel");
        Path localJar = worker.resolve("connectors/plugin with spaces.jar");
        createArtifact(localJar);
        URL original = master.resolve("connectors/plugin with spaces.jar").toUri().toURL();
        assertEquals(
                Collections.singletonList(localJar.toUri().toURL()),
                resolver(master, worker).resolve(Collections.singletonList(original)));
    }

    @Test
    void preservesSiblingAndNonLocalUrls() throws Exception {
        Path master = directory.resolve("master");
        Path worker = directory.resolve("worker");
        URL sibling = directory.resolve("master-other/connectors/plugin.jar").toUri().toURL();
        URL remote = new URL("https://example.invalid/plugin.jar");
        URL remoteFile = new URL("file://remote-host/distribution/plugin.jar");
        assertEquals(
                Arrays.asList(sibling, remote, remoteFile),
                resolver(master, worker).resolve(Arrays.asList(sibling, remote, remoteFile)));
    }

    @Test
    void rejectsMissingJarAndTraversalOutsideMasterRoot() throws Exception {
        Path master = directory.resolve("master");
        Path worker = directory.resolve("worker");
        Files.createDirectories(worker);
        ApplicationJarPathResolver resolver = resolver(master, worker);
        URL missing = master.resolve("connectors/missing.jar").toUri().toURL();
        assertThrows(IOException.class, () -> resolver.resolve(Collections.singletonList(missing)));
        URL traversal = new URL(master.toUri().toString() + "/../outside.jar");
        assertThrows(
                IOException.class, () -> resolver.resolve(Collections.singletonList(traversal)));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void rejectsSymlinkOutsideWorkerRoot() throws Exception {
        Path master = directory.resolve("master");
        Path worker = directory.resolve("worker");
        Path external = directory.resolve("external.jar");
        createArtifact(external);
        Files.createDirectories(worker.resolve("connectors"));
        Files.createSymbolicLink(worker.resolve("connectors/plugin.jar"), external);
        URL original = master.resolve("connectors/plugin.jar").toUri().toURL();
        assertThrows(
                IOException.class,
                () -> resolver(master, worker).resolve(Collections.singletonList(original)));
    }

    @Test
    void requiresExplicitAbsoluteRoots() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ApplicationJarPathResolver("relative", directory.toString()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ApplicationJarPathResolver(directory.toString(), "relative"));
        assertThrows(
                NullPointerException.class,
                () -> new ApplicationJarPathResolver(null, directory.toString()));
    }

    private ApplicationJarPathResolver resolver(Path master, Path worker) {
        return new ApplicationJarPathResolver(master.toString(), worker.toString());
    }

    private void createArtifact(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[] {1});
    }
}
