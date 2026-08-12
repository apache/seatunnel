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

package org.apache.seatunnel.e2e.connector.paimon;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import com.mysql.cj.jdbc.Driver;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class PaimonDependencyResolver {

    private static final String DEPENDENCY_RESOURCE_DIRECTORY = "e2e-dependencies/";

    private PaimonDependencyResolver() {}

    static void copyMySqlDriverToContainer(GenericContainer<?> container, String targetDirectory)
            throws IOException, InterruptedException {
        copyDependencyToContainer(container, Driver.class, targetDirectory);
    }

    static void copyHiveDependenciesToContainer(
            GenericContainer<?> container, String targetDirectory)
            throws IOException, InterruptedException {
        copyMavenDependencyToContainer(container, "hive-exec.jar", targetDirectory);
        copyMavenDependencyToContainer(container, "libfb303.jar", targetDirectory);
    }

    static void addS3DependenciesToContainer(
            GenericContainer<?> container, String targetDirectory) {
        addMavenDependencyToContainer(container, "aws-java-sdk-bundle.jar", targetDirectory);
        addMavenDependencyToContainer(container, "hadoop-aws.jar", targetDirectory);
    }

    private static void copyDependencyToContainer(
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

    private static void copyMavenDependencyToContainer(
            GenericContainer<?> container, String dependency, String targetDirectory)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + targetDirectory);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

        Path dependencyJar = mavenDependencyJarPath(dependency);
        container.copyFileToContainer(
                MountableFile.forHostPath(dependencyJar),
                targetDirectory + "/" + dependencyJar.getFileName());
    }

    private static void addMavenDependencyToContainer(
            GenericContainer<?> container, String dependency, String targetDirectory) {
        Path dependencyJar = mavenDependencyJarPath(dependency);
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

    private static Path mavenDependencyJarPath(String dependency) {
        try {
            URL dependencyResource =
                    PaimonDependencyResolver.class
                            .getClassLoader()
                            .getResource(DEPENDENCY_RESOURCE_DIRECTORY + dependency);
            Assertions.assertNotNull(
                    dependencyResource, "Maven dependency copy output is missing: " + dependency);
            Path dependencyJar = Paths.get(dependencyResource.toURI());
            Assertions.assertTrue(
                    Files.isRegularFile(dependencyJar),
                    "Maven dependency copy output should be a file: " + dependencyJar);
            return dependencyJar;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve Maven-copied dependency " + dependency, e);
        }
    }
}
