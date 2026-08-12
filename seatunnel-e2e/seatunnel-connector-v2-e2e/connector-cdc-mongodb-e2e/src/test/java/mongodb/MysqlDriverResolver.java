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

package mongodb;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import com.mysql.cj.jdbc.Driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class MysqlDriverResolver {

    private static final String JDBC_PLUGIN_LIB = "/tmp/seatunnel/plugins/Jdbc/lib";

    private MysqlDriverResolver() {}

    static void copyMySQLDriverToJdbcContainer(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + JDBC_PLUGIN_LIB);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

        Path driverJarPath = mysqlDriverJarPath();
        container.copyFileToContainer(
                MountableFile.forHostPath(driverJarPath),
                JDBC_PLUGIN_LIB + "/" + driverJarPath.getFileName());
    }

    private static Path mysqlDriverJarPath() {
        try {
            Path driverJarPath =
                    Paths.get(
                            Driver.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            Assertions.assertTrue(Files.isRegularFile(driverJarPath));
            return driverJarPath;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve MySQL JDBC driver jar from the test classpath", e);
        }
    }
}
