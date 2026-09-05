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

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** A test dependency jar that can be copied into an E2E container. */
public final class DependencyJar {

    private static final String STAGED_RESOURCE_DIRECTORY = "e2e-dependencies/";

    private final Path path;

    private DependencyJar(Path path) {
        this.path = path;
    }

    public static DependencyJar of(Class<?> dependencyClass) {
        try {
            URL location = dependencyClass.getProtectionDomain().getCodeSource().getLocation();
            return new DependencyJar(
                    requireJar(
                            Paths.get(location.toURI()),
                            "Maven dependency for " + dependencyClass.getName()));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve Maven dependency for " + dependencyClass.getName(), e);
        }
    }

    public static DependencyJar ofClassName(String dependencyClassName) {
        try {
            return of(Class.forName(dependencyClassName));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Failed to load Maven dependency class " + dependencyClassName, e);
        }
    }

    public static DependencyJar staged(String fileName) {
        try {
            URL resource =
                    DependencyJar.class
                            .getClassLoader()
                            .getResource(STAGED_RESOURCE_DIRECTORY + fileName);
            if (resource == null) {
                throw new IllegalStateException(
                        "Maven dependency should be staged at "
                                + STAGED_RESOURCE_DIRECTORY
                                + fileName);
            }
            return new DependencyJar(
                    requireJar(Paths.get(resource.toURI()), "Staged Maven dependency " + fileName));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve staged Maven dependency " + fileName, e);
        }
    }

    public Path path() {
        return path;
    }

    /** Copies this dependency into an already-running container. */
    public void copyTo(GenericContainer<?> container, String targetDirectory)
            throws IOException, InterruptedException {
        copyTo(container, targetDirectory, path.getFileName().toString());
    }

    /** Copies this dependency into an already-running container with the given file name. */
    public void copyTo(GenericContainer<?> container, String targetDirectory, String targetFileName)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + targetDirectory);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());
        container.copyFileToContainer(
                MountableFile.forHostPath(path), targetDirectory + "/" + targetFileName);
    }

    /** Registers this dependency to be copied when the container starts. */
    public void addTo(GenericContainer<?> container, String targetDirectory) {
        container.withCopyFileToContainer(
                MountableFile.forHostPath(path), targetDirectory + "/" + path.getFileName());
    }

    private static Path requireJar(Path path, String description) {
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException(description + " should be a jar: " + path);
        }
        return path;
    }
}
