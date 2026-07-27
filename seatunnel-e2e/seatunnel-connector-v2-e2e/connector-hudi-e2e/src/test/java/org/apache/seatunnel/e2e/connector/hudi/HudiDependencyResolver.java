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

package org.apache.seatunnel.e2e.connector.hudi;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class HudiDependencyResolver {

    private HudiDependencyResolver() {}

    static void copyDependencyToContainer(
            GenericContainer<?> container, Class<?> dependencyClass, String targetDirectory)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + targetDirectory);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

        Path dependencyJar = dependencyJarPath(dependencyClass);
        container.copyFileToContainer(
                MountableFile.forHostPath(dependencyJar),
                targetDirectory + "/" + dependencyJar.getFileName());
    }

    static void addDependencyToContainer(
            GenericContainer<?> container, Class<?> dependencyClass, String targetDirectory) {
        Path dependencyJar = dependencyJarPath(dependencyClass);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(dependencyJar),
                targetDirectory + "/" + dependencyJar.getFileName());
    }

    private static Path dependencyJarPath(Class<?> dependencyClass) {
        try {
            Path dependencyJar =
                    Paths.get(
                            dependencyClass
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            Assertions.assertTrue(
                    Files.isRegularFile(dependencyJar),
                    "Dependency should be resolved from the test classpath: " + dependencyJar);
            return dependencyJar;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve dependency jar for " + dependencyClass.getName(), e);
        }
    }
}
