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

package org.apache.seatunnel.connectors.jdbc;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class JdbcDdlDriverResolver {

    private static final String JDBC_PLUGIN_LIB = "/tmp/seatunnel/plugins/Jdbc/lib";

    private JdbcDdlDriverResolver() {}

    static void copyDriverToContainer(
            GenericContainer<?> container, String driverClassName, String targetDirectory)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + targetDirectory);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

        Path driverJar = driverJarPath(driverClassName);
        container.copyFileToContainer(
                MountableFile.forHostPath(driverJar),
                targetDirectory + "/" + driverJar.getFileName());
    }

    static void copyDriverToContainer(GenericContainer<?> container, String driverClassName)
            throws IOException, InterruptedException {
        copyDriverToContainer(container, driverClassName, JDBC_PLUGIN_LIB);
    }

    private static Path driverJarPath(String driverClassName) {
        try {
            Class<?> driverClass = Class.forName(driverClassName);
            Path driverJar =
                    Paths.get(
                            driverClass
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            Assertions.assertTrue(
                    Files.isRegularFile(driverJar),
                    "JDBC driver should be resolved from the test classpath: " + driverJar);
            return driverJar;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve JDBC driver jar for " + driverClassName, e);
        }
    }
}
