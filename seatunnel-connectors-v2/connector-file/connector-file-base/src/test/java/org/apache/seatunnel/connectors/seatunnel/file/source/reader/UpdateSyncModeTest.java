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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason =
                "Hadoop has windows problem, please refer https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems")
class UpdateSyncModeTest {

    @TempDir Path tempDir;

    @Test
    void testDistcpDoesNotSupportChecksumCompareMode() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            Assertions.assertThrows(
                    FileConnectorException.class,
                    () ->
                            strategy.setPluginConfig(
                                    updateConfig(
                                            sourceDir.toUri().toString(),
                                            targetDir.toUri().toString(),
                                            "distcp",
                                            "checksum")));
        }
    }

    @Test
    void testUpdateModeOnlySupportsBinaryFormat() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("path", sourceDir.toUri().toString());
        configMap.put("file_format_type", "text");
        configMap.put("sync_mode", "update");
        configMap.put("target_path", targetDir.toUri().toString());

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            Assertions.assertThrows(
                    FileConnectorException.class,
                    () -> strategy.setPluginConfig(ConfigFactory.parseMap(configMap)));
        }
    }

    @Test
    void testUpdateModeRequiresTargetPath() throws Exception {
        Path sourceDir = tempDir.resolve("src");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("path", sourceDir.toUri().toString());
        configMap.put("file_format_type", "binary");
        configMap.put("sync_mode", "update");

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            Assertions.assertThrows(
                    FileConnectorException.class,
                    () -> strategy.setPluginConfig(ConfigFactory.parseMap(configMap)));
        }
    }

    @Test
    void testDistcpSkipWhenTargetNewerAndSameLength() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");
        Path sourceFile = sourceDir.resolve("a/b/test.bin");
        Path targetFile = targetDir.resolve("a/b/test.bin");

        writeFile(sourceFile, "abc".getBytes());
        writeFile(targetFile, "abc".getBytes());
        setMtime(sourceFile, 1_000);
        setMtime(targetFile, 2_000);

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            strategy.setPluginConfig(
                    updateConfig(
                            sourceDir.toUri().toString(),
                            targetDir.toUri().toString(),
                            "distcp",
                            "len_mtime"));
            strategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> files = strategy.getFileNamesByPath(sourceDir.toUri().toString());
            Assertions.assertTrue(files.isEmpty(), "Target is newer with same len -> SKIP");
        }
    }

    @Test
    void testDistcpCopyWhenSourceNewer() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");
        Path sourceFile = sourceDir.resolve("test.bin");
        Path targetFile = targetDir.resolve("test.bin");

        writeFile(sourceFile, "abc".getBytes());
        writeFile(targetFile, "abc".getBytes());
        setMtime(sourceFile, 2_000);
        setMtime(targetFile, 1_000);

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            strategy.setPluginConfig(
                    updateConfig(
                            sourceDir.toUri().toString(),
                            targetDir.toUri().toString(),
                            "distcp",
                            "len_mtime"));
            strategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> files = strategy.getFileNamesByPath(sourceDir.toUri().toString());
            Assertions.assertEquals(1, files.size());
            Assertions.assertTrue(files.get(0).endsWith("/test.bin"));
        }
    }

    @Test
    void testStrictChecksumSkipWhenSameContentEvenIfMtimeDiff() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");
        Path sourceFile = sourceDir.resolve("test.bin");
        Path targetFile = targetDir.resolve("test.bin");

        writeFile(sourceFile, "abc".getBytes());
        writeFile(targetFile, "abc".getBytes());
        setMtime(sourceFile, 1_000);
        setMtime(targetFile, 2_000);

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            strategy.setPluginConfig(
                    updateConfig(
                            sourceDir.toUri().toString(),
                            targetDir.toUri().toString(),
                            "strict",
                            "checksum"));
            strategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> files = strategy.getFileNamesByPath(sourceDir.toUri().toString());
            Assertions.assertTrue(files.isEmpty(), "Checksum equal -> SKIP");
        }
    }

    @Test
    void testStrictChecksumCopyWhenSameLengthButDifferentContent() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");
        Path sourceFile = sourceDir.resolve("test.bin");
        Path targetFile = targetDir.resolve("test.bin");

        writeFile(sourceFile, "abc".getBytes());
        writeFile(targetFile, "abd".getBytes());

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            strategy.setPluginConfig(
                    updateConfig(
                            sourceDir.toUri().toString(),
                            targetDir.toUri().toString(),
                            "strict",
                            "checksum"));
            strategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> files = strategy.getFileNamesByPath(sourceDir.toUri().toString());
            Assertions.assertEquals(1, files.size());
            Assertions.assertTrue(files.get(0).endsWith("/test.bin"));
        }
    }

    @Test
    void testUpdateModeNonRecursiveScanOnlyComparesTopLevelFiles() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");
        Path topLevelSourceFile = sourceDir.resolve("root.bin");
        Path nestedSourceFile = sourceDir.resolve("subdir/nested.bin");
        Path topLevelTargetFile = targetDir.resolve("root.bin");
        Path nestedTargetFile = targetDir.resolve("subdir/nested.bin");

        writeFile(topLevelSourceFile, "root".getBytes());
        writeFile(nestedSourceFile, "nested".getBytes());
        writeFile(topLevelTargetFile, "root".getBytes());
        writeFile(nestedTargetFile, "nested".getBytes());
        setMtime(topLevelSourceFile, 2_000);
        setMtime(topLevelTargetFile, 1_000);
        setMtime(nestedSourceFile, 2_000);
        setMtime(nestedTargetFile, 1_000);

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            strategy.setPluginConfig(
                    updateConfig(
                            sourceDir.toUri().toString(),
                            targetDir.toUri().toString(),
                            "distcp",
                            "len_mtime",
                            false));
            strategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> files = strategy.getFileNamesByPath(sourceDir.toUri().toString());
            Assertions.assertEquals(1, files.size());
            Assertions.assertTrue(files.get(0).endsWith("/root.bin"));
        }
    }

    @Test
    void testUpdateModeNonRecursiveScanSkipsNestedChanges() throws Exception {
        Path sourceDir = tempDir.resolve("src");
        Path targetDir = tempDir.resolve("dst");
        Path topLevelSourceFile = sourceDir.resolve("root.bin");
        Path nestedSourceFile = sourceDir.resolve("subdir/nested.bin");
        Path topLevelTargetFile = targetDir.resolve("root.bin");
        Path nestedTargetFile = targetDir.resolve("subdir/nested.bin");

        writeFile(topLevelSourceFile, "root".getBytes());
        writeFile(nestedSourceFile, "nested".getBytes());
        writeFile(topLevelTargetFile, "root".getBytes());
        writeFile(nestedTargetFile, "nested".getBytes());
        setMtime(topLevelSourceFile, 1_000);
        setMtime(topLevelTargetFile, 1_000);
        setMtime(nestedSourceFile, 2_000);
        setMtime(nestedTargetFile, 1_000);

        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            strategy.setPluginConfig(
                    updateConfig(
                            sourceDir.toUri().toString(),
                            targetDir.toUri().toString(),
                            "distcp",
                            "len_mtime",
                            false));
            strategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> files = strategy.getFileNamesByPath(sourceDir.toUri().toString());
            Assertions.assertTrue(files.isEmpty(), "Nested-only changes should be skipped");
        }
    }

    private static void writeFile(Path path, byte[] content) throws IOException {
        Files.createDirectories(path.getParent());
        Files.write(path, content);
    }

    private static void setMtime(Path path, long millis) throws IOException {
        Files.setLastModifiedTime(path, FileTime.fromMillis(millis));
    }

    private static Config updateConfig(
            String sourcePath, String targetPath, String updateStrategy, String compareMode) {
        return updateConfig(sourcePath, targetPath, updateStrategy, compareMode, true);
    }

    private static Config updateConfig(
            String sourcePath,
            String targetPath,
            String updateStrategy,
            String compareMode,
            boolean recursiveFileScan) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("path", sourcePath);
        configMap.put("file_format_type", "binary");
        configMap.put("sync_mode", "update");
        configMap.put("target_path", targetPath);
        configMap.put("update_strategy", updateStrategy);
        configMap.put("compare_mode", compareMode);
        configMap.put("recursive_file_scan", recursiveFileScan);
        return ConfigFactory.parseMap(configMap);
    }
}
