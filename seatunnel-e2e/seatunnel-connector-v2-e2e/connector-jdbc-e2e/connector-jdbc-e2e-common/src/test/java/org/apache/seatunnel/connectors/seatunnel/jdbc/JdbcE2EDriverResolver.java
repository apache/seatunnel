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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class JdbcE2EDriverResolver {

    static final String JDBC_PLUGIN_LIB = "/tmp/seatunnel/plugins/Jdbc/lib";

    private JdbcE2EDriverResolver() {}

    static void copyDriverToContainer(GenericContainer<?> container, String driverClassName)
            throws IOException, InterruptedException {
        copyDriverToContainer(container, driverClassName, JDBC_PLUGIN_LIB);
    }

    static void copyDriverToContainer(
            GenericContainer<?> container, String driverClassName, String targetDirectory)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + targetDirectory);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

        Path driverJarPath = driverJarPath(driverClassName);
        container.copyFileToContainer(
                MountableFile.forHostPath(driverJarPath),
                targetDirectory + "/" + driverJarPath.getFileName());
    }

    static Path driverJarPath(String driverClassName) {
        try {
            Class<?> driverClass = Class.forName(driverClassName);
            Path driverJarPath =
                    Paths.get(
                            driverClass
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            Assertions.assertTrue(
                    Files.isRegularFile(driverJarPath),
                    "JDBC driver should be resolved from the test classpath: " + driverJarPath);
            return driverJarPath;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve JDBC driver jar for " + driverClassName, e);
        }
    }
}
