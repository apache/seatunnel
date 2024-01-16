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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.command.Command;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtilsTest {

    @Test
    public void getConfigPath() throws URISyntaxException {
        // test client mode.
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);
        Path expectConfPath =
                Paths.get(FileUtilsTest.class.getResource("/flink.batch.conf").toURI());
        sparkCommandArgs.setConfigFile(expectConfPath.toString());
        Assertions.assertEquals(expectConfPath, FileUtils.getConfigPath(sparkCommandArgs));

        // test cluster mode
        sparkCommandArgs.setDeployMode(DeployMode.CLUSTER);
        Assertions.assertEquals(
                "flink.batch.conf", FileUtils.getConfigPath(sparkCommandArgs).toString());
    }

    @Test
    void testExpectedError() {
        String root = System.getProperty("java.io.tmpdir");
        // Unix Path: /tmp/not/existed
        // Windows Path: %SystemDrive%\Users\<username>\AppData\Local\Temp\not\existed
        Path path = Paths.get(root, "not", "existed");
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> FileUtils.checkConfigExist(path));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-22], ErrorDescription:[SeaTunnel read file '"
                        + path
                        + "' failed, because it not existed.]",
                exception.getMessage());
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    private static class SparkCommandArgs extends AbstractCommandArgs {

        @Parameter(
                names = {"-c", "--config"},
                description = "Config file",
                required = true)
        private String configFile;

        private DeployMode deployMode;

        @Override
        public Command<?> buildCommand() {
            return null;
        }
    }
}
