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

package org.apache.seatunnel.core.starter.spark;

import org.apache.seatunnel.common.constants.EngineType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class SparkStarterTest {

    @TempDir Path tempDir;

    @AfterEach
    void clearStarterJarName() {
        System.clearProperty(SparkStarter.STARTER_JAR_NAME_PROPERTY);
        System.clearProperty(SparkStarter.ARGS_FILE_PROPERTY);
    }

    @Test
    void useSpark3StarterJarByDefault() {
        Assertions.assertEquals(
                EngineType.SPARK3.getStarterJarName(), SparkStarter.getStarterJarName());
    }

    @Test
    void useConfiguredStarterJarName() {
        System.setProperty(
                SparkStarter.STARTER_JAR_NAME_PROPERTY, SparkStarter.SPARK_35_STARTER_JAR_NAME);

        Assertions.assertEquals(
                SparkStarter.SPARK_35_STARTER_JAR_NAME, SparkStarter.getStarterJarName());
    }

    @Test
    void preserveLegacyCommandFormatting() {
        SparkStarter starter = SparkStarter.getInstance(new String[] {"--config", "job.conf"});
        List<String> command = new ArrayList<>();
        starter.appendOption(command, "--name", "a \"quoted\" job");
        Assertions.assertEquals(Arrays.asList("--name", "\"a \\\"quoted\\\" job\""), command);

        starter.sparkConf = Collections.emptyMap();
        starter.commandArgs.setVariables(Collections.singletonList("table=orders archive"));
        Assertions.assertTrue(starter.buildFinal().contains("-i table=orders archive"));
        Assertions.assertEquals("${SPARK_HOME}/bin/spark-submit", starter.buildFinal().get(0));
    }

    @Test
    void keepRawOptionsAndVariablesAsSeparateArguments() {
        System.setProperty(SparkStarter.ARGS_FILE_PROPERTY, tempDir.resolve("args").toString());
        SparkStarter starter = SparkStarter.getInstance(new String[] {"--config", "job file.conf"});
        String value = "orders $(touch marker) `id` $HOME * \"quoted\"\nnext line";
        starter.sparkConf = Collections.singletonMap("spark.app.name", value);
        starter.commandArgs.setVariables(Collections.singletonList("table=" + value));
        List<String> command = starter.buildFinal();

        Assertions.assertEquals(
                "spark.app.name=" + value, command.get(command.indexOf("--conf") + 1));
        Assertions.assertEquals("job file.conf", command.get(command.indexOf("--config") + 1));
        Assertions.assertEquals("table=" + value, command.get(command.indexOf("-i") + 1));
        List<String> driverOption = new ArrayList<>();
        starter.appendOption(driverOption, " --driver-library-path", "/path with spaces");
        Assertions.assertEquals(
                Arrays.asList("--driver-library-path", "/path with spaces"), driverOption);
    }

    @Test
    void writeNulDelimitedArguments() throws IOException {
        Path path = tempDir.resolve("args");
        SparkStarter.writeArguments(path, Arrays.asList("--name", "", "a\nb", "quoted \" '$HOME'"));
        Assertions.assertArrayEquals(
                "--name\0\0a\nb\0quoted \" '$HOME'\0".getBytes(StandardCharsets.UTF_8),
                Files.readAllBytes(path));
    }

    @Test
    void rejectNulInsideAnArgument() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SparkStarter.writeArguments(
                                tempDir.resolve("args"), Collections.singletonList("a\0b")));
    }
}
