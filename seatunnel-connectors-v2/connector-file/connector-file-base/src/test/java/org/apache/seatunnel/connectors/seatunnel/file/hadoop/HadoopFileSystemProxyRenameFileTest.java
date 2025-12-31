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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileExistsMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@DisabledOnOs(OS.WINDOWS)
class HadoopFileSystemProxyRenameFileTest {

    @TempDir private Path tempDir;

    @Test
    void testRenameOverwriteWhenTargetExists() throws Exception {
        Path oldFile = tempDir.resolve("old.txt");
        Path newFile = tempDir.resolve("new.txt");
        Files.write(oldFile, "old".getBytes(StandardCharsets.UTF_8));
        Files.write(newFile, "existing".getBytes(StandardCharsets.UTF_8));

        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            proxy.renameFile(
                    oldFile.toUri().toString(),
                    newFile.toUri().toString(),
                    FileExistsMode.OVERWRITE);
        }

        Assertions.assertFalse(Files.exists(oldFile));
        Assertions.assertEquals(
                "old", new String(Files.readAllBytes(newFile), StandardCharsets.UTF_8));
    }

    @Test
    void testRenameSkipDeletesTempFileWhenTargetExists() throws Exception {
        Path oldFile = tempDir.resolve("old.txt");
        Path newFile = tempDir.resolve("new.txt");
        Files.write(oldFile, "new".getBytes(StandardCharsets.UTF_8));
        Files.write(newFile, "existing".getBytes(StandardCharsets.UTF_8));

        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            proxy.renameFile(
                    oldFile.toUri().toString(), newFile.toUri().toString(), FileExistsMode.SKIP);
        }

        Assertions.assertFalse(Files.exists(oldFile));
        Assertions.assertEquals(
                "existing", new String(Files.readAllBytes(newFile), StandardCharsets.UTF_8));
    }

    @Test
    void testRenameFailWhenTargetExists() throws Exception {
        Path oldFile = tempDir.resolve("old.txt");
        Path newFile = tempDir.resolve("new.txt");
        Files.write(oldFile, "new".getBytes(StandardCharsets.UTF_8));
        Files.write(newFile, "existing".getBytes(StandardCharsets.UTF_8));

        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            Assertions.assertThrows(
                    SeaTunnelRuntimeException.class,
                    () ->
                            proxy.renameFile(
                                    oldFile.toUri().toString(),
                                    newFile.toUri().toString(),
                                    FileExistsMode.FAIL));
        }

        Assertions.assertTrue(Files.exists(oldFile));
        Assertions.assertEquals(
                "existing", new String(Files.readAllBytes(newFile), StandardCharsets.UTF_8));
    }

    @Test
    void testRenameTargetExistsButIsDirectory() throws Exception {
        Path oldFile = tempDir.resolve("old.txt");
        Path targetDir = tempDir.resolve("targetDir");
        Files.createDirectories(targetDir);
        Files.write(oldFile, "new".getBytes(StandardCharsets.UTF_8));

        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            Assertions.assertThrows(
                    SeaTunnelRuntimeException.class,
                    () ->
                            proxy.renameFile(
                                    oldFile.toUri().toString(),
                                    targetDir.toUri().toString(),
                                    FileExistsMode.OVERWRITE));
        }
    }
}
