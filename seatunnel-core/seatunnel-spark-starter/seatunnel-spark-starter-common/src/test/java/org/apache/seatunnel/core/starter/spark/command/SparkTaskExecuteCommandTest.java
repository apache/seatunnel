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

package org.apache.seatunnel.core.starter.spark.command;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

class SparkTaskExecuteCommandTest {

    @TempDir private Path tempDir;

    @Test
    void resolveClusterConfigFromSparkFilesWhenLocalBasenameMissing() throws IOException {
        Path shippedConfig = Files.createFile(tempDir.resolve("v2.batch.config"));
        SparkCommandArgs args = new SparkCommandArgs();
        args.setDeployMode(DeployMode.CLUSTER);
        args.setConfigFile("/opt/seatunnel/config/v2.batch.config");

        Path resolvedConfig =
                SparkTaskExecuteCommand.resolveConfigPath(
                        args, fileName -> tempDir.resolve(fileName).toString());

        Assertions.assertEquals(shippedConfig, resolvedConfig);
    }

    @Test
    void keepClientConfigPathWithoutCallingSparkFiles() throws IOException {
        Path clientConfig = Files.createFile(tempDir.resolve("client.conf"));
        SparkCommandArgs args = new SparkCommandArgs();
        args.setDeployMode(DeployMode.CLIENT);
        args.setConfigFile(clientConfig.toString());
        AtomicBoolean sparkFilesCalled = new AtomicBoolean(false);

        Path resolvedConfig =
                SparkTaskExecuteCommand.resolveConfigPath(
                        args,
                        fileName -> {
                            sparkFilesCalled.set(true);
                            return tempDir.resolve(fileName).toString();
                        });

        Assertions.assertEquals(clientConfig, resolvedConfig);
        Assertions.assertFalse(sparkFilesCalled.get());
    }

    @Test
    void keepClusterConfigPathWhenSparkFilesReturnsNull() {
        SparkCommandArgs args = new SparkCommandArgs();
        args.setDeployMode(DeployMode.CLUSTER);
        args.setConfigFile("/opt/seatunnel/config/missing.conf");

        Path resolvedConfig = SparkTaskExecuteCommand.resolveConfigPath(args, fileName -> null);

        Assertions.assertEquals(Paths.get("missing.conf"), resolvedConfig);
    }

    @Test
    void keepClusterConfigPathWhenSparkFilesThrowsException() {
        SparkCommandArgs args = new SparkCommandArgs();
        args.setDeployMode(DeployMode.CLUSTER);
        args.setConfigFile("/opt/seatunnel/config/missing.conf");

        Path resolvedConfig =
                SparkTaskExecuteCommand.resolveConfigPath(
                        args,
                        fileName -> {
                            throw new NullPointerException("SparkEnv is not initialized");
                        });

        Assertions.assertEquals(Paths.get("missing.conf"), resolvedConfig);
    }

    @Test
    void keepClusterConfigPathWhenSparkFilesPathDoesNotExist() {
        SparkCommandArgs args = new SparkCommandArgs();
        args.setDeployMode(DeployMode.CLUSTER);
        args.setConfigFile("/opt/seatunnel/config/missing.conf");

        Path resolvedConfig =
                SparkTaskExecuteCommand.resolveConfigPath(
                        args, fileName -> tempDir.resolve(fileName).toString());

        Assertions.assertEquals(Paths.get("missing.conf"), resolvedConfig);
    }

    @Test
    void preferSparkFilesConfigWhenLocalBasenameExists() throws IOException {
        Path localConfig = Paths.get("spark-local-basename-exists.conf");
        Path shippedConfig = Files.createFile(tempDir.resolve(localConfig.getFileName()));
        Files.createFile(localConfig);
        try {
            SparkCommandArgs args = new SparkCommandArgs();
            args.setDeployMode(DeployMode.CLUSTER);
            args.setConfigFile("/opt/seatunnel/config/" + localConfig.getFileName());

            Path resolvedConfig =
                    SparkTaskExecuteCommand.resolveConfigPath(
                            args, fileName -> tempDir.resolve(fileName).toString());

            Assertions.assertEquals(shippedConfig, resolvedConfig);
        } finally {
            Files.deleteIfExists(localConfig);
        }
    }
}
