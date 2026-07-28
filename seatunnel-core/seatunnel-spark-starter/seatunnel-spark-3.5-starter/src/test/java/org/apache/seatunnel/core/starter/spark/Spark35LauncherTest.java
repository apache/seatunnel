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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Spark35LauncherTest {

    @Test
    public void resolveWindowsApplicationDirectoryFromBinDirectory() throws IOException {
        String launcher =
                new String(Files.readAllBytes(locateWindowsLauncher()), StandardCharsets.UTF_8);

        Assertions.assertTrue(
                launcher.contains("for %%D in (\"%PRG_DIR%..\") do set \"APP_DIR=%%~fD\""));
        Assertions.assertFalse(launcher.contains("set \"APP_DIR=%~dp0\""));
        Assertions.assertTrue(launcher.contains("set \"CONF_DIR=%APP_DIR%\\config\""));
        Assertions.assertTrue(
                launcher.contains("set \"APP_JAR=%APP_DIR%\\starter\\%APP_JAR_NAME%\""));
    }

    private Path locateWindowsLauncher() {
        Path modulePath = Paths.get("src/main/bin/start-seatunnel-spark-3.5-connector-v2.cmd");
        if (Files.exists(modulePath)) {
            return modulePath;
        }
        return Paths.get(
                "seatunnel-core/seatunnel-spark-starter/seatunnel-spark-3.5-starter/"
                        + "src/main/bin/start-seatunnel-spark-3.5-connector-v2.cmd");
    }
}
