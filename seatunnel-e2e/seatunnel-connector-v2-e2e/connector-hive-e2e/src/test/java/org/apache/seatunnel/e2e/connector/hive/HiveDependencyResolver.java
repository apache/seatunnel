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

package org.apache.seatunnel.e2e.connector.hive;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class HiveDependencyResolver {

    private static final String DEPENDENCY_RESOURCE_DIRECTORY = "e2e-dependencies/";
    private static final String[] HIVE_DEPENDENCIES = {
        "hive-exec.jar",
        "libfb303.jar",
        "hadoop-aws.jar",
        "aliyun-sdk-oss.jar",
        "jdom.jar",
        "hadoop-aliyun.jar",
        "hadoop-cos.jar"
    };

    private HiveDependencyResolver() {}

    static void copyHiveDependenciesToContainer(
            GenericContainer<?> container, String targetDirectory)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + targetDirectory);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

        for (String dependency : HIVE_DEPENDENCIES) {
            Path dependencyJar = dependencyJarPath(dependency);
            container.copyFileToContainer(
                    MountableFile.forHostPath(dependencyJar),
                    targetDirectory + "/" + dependencyJar.getFileName());
        }
    }

    private static Path dependencyJarPath(String dependency) {
        try {
            URL dependencyResource =
                    HiveDependencyResolver.class
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
