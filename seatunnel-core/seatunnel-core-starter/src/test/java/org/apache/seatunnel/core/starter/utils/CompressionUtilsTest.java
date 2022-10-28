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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class CompressionUtilsTest {

    @Test
    public void tar() throws IOException {
        Path pluginRootDir = Files.createTempDirectory("plugins_");
        Path outputFile = Files.createTempFile("plugins_", ".tar.gz");
        Path pluginDir = Files.createDirectory(pluginRootDir.resolve("plugin1"));
        Path pluginLibDir = Files.createDirectory(pluginDir.resolve("lib"));
        Files.createFile(pluginLibDir.resolve("a.jar"));
        Files.createFile(pluginLibDir.resolve("b.jar"));
        CompressionUtils.tarGzip(pluginRootDir, outputFile);
        assertTrue(Files.exists(outputFile));

        Files.walk(pluginRootDir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);

        Files.delete(outputFile);
    }
}
