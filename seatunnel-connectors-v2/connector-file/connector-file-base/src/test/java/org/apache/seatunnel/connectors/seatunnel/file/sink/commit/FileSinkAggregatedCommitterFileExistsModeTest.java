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

package org.apache.seatunnel.connectors.seatunnel.file.sink.commit;

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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

@DisabledOnOs(OS.WINDOWS)
class FileSinkAggregatedCommitterFileExistsModeTest {

    @TempDir private Path tempDir;

    @Test
    void testCommitOverwriteWhenTargetExists() throws Exception {
        Path transactionDir = tempDir.resolve("txn");
        Files.createDirectories(transactionDir);
        Path tempFile = transactionDir.resolve("tmp.txt");
        Files.write(tempFile, "new".getBytes(StandardCharsets.UTF_8));

        Path targetDir = tempDir.resolve("target");
        Files.createDirectories(targetDir);
        Path targetFile = targetDir.resolve("out.txt");
        Files.write(targetFile, "existing".getBytes(StandardCharsets.UTF_8));

        FileAggregatedCommitInfo commitInfo = buildCommitInfo(transactionDir, tempFile, targetFile);
        FileSinkAggregatedCommitter committer =
                new FileSinkAggregatedCommitter(
                        new HadoopConf("file:///"), FileExistsMode.OVERWRITE);
        committer.init();
        try {
            List<FileAggregatedCommitInfo> errors =
                    committer.commit(Collections.singletonList(commitInfo));
            Assertions.assertTrue(errors.isEmpty());
        } finally {
            committer.close();
        }

        Assertions.assertFalse(Files.exists(tempFile));
        Assertions.assertFalse(Files.exists(transactionDir));
        Assertions.assertEquals(
                "new", new String(Files.readAllBytes(targetFile), StandardCharsets.UTF_8));
    }

    @Test
    void testCommitSkipDeletesTempFileWhenTargetExists() throws Exception {
        Path transactionDir = tempDir.resolve("txn");
        Files.createDirectories(transactionDir);
        Path tempFile = transactionDir.resolve("tmp.txt");
        Files.write(tempFile, "new".getBytes(StandardCharsets.UTF_8));

        Path targetDir = tempDir.resolve("target");
        Files.createDirectories(targetDir);
        Path targetFile = targetDir.resolve("out.txt");
        Files.write(targetFile, "existing".getBytes(StandardCharsets.UTF_8));

        FileAggregatedCommitInfo commitInfo = buildCommitInfo(transactionDir, tempFile, targetFile);
        FileSinkAggregatedCommitter committer =
                new FileSinkAggregatedCommitter(new HadoopConf("file:///"), FileExistsMode.SKIP);
        committer.init();
        try {
            List<FileAggregatedCommitInfo> errors =
                    committer.commit(Collections.singletonList(commitInfo));
            Assertions.assertTrue(errors.isEmpty());
        } finally {
            committer.close();
        }

        Assertions.assertFalse(Files.exists(tempFile));
        Assertions.assertFalse(Files.exists(transactionDir));
        Assertions.assertEquals(
                "existing", new String(Files.readAllBytes(targetFile), StandardCharsets.UTF_8));
    }

    @Test
    void testCommitFailWhenTargetExists() throws Exception {
        Path transactionDir = tempDir.resolve("txn");
        Files.createDirectories(transactionDir);
        Path tempFile = transactionDir.resolve("tmp.txt");
        Files.write(tempFile, "new".getBytes(StandardCharsets.UTF_8));

        Path targetDir = tempDir.resolve("target");
        Files.createDirectories(targetDir);
        Path targetFile = targetDir.resolve("out.txt");
        Files.write(targetFile, "existing".getBytes(StandardCharsets.UTF_8));

        FileAggregatedCommitInfo commitInfo = buildCommitInfo(transactionDir, tempFile, targetFile);
        FileSinkAggregatedCommitter committer =
                new FileSinkAggregatedCommitter(new HadoopConf("file:///"), FileExistsMode.FAIL);
        committer.init();
        try {
            List<FileAggregatedCommitInfo> errors =
                    committer.commit(Collections.singletonList(commitInfo));
            Assertions.assertEquals(1, errors.size());
        } finally {
            committer.close();
        }

        Assertions.assertTrue(Files.exists(tempFile));
        Assertions.assertTrue(Files.exists(transactionDir));
        Assertions.assertEquals(
                "existing", new String(Files.readAllBytes(targetFile), StandardCharsets.UTF_8));
    }

    private FileAggregatedCommitInfo buildCommitInfo(
            Path transactionDir, Path tempFile, Path targetFile) {
        LinkedHashMap<String, String> needMoveFiles = new LinkedHashMap<>();
        needMoveFiles.put(tempFile.toUri().toString(), targetFile.toUri().toString());

        LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap = new LinkedHashMap<>();
        transactionMap.put(transactionDir.toUri().toString(), needMoveFiles);
        return new FileAggregatedCommitInfo(transactionMap, new LinkedHashMap<>());
    }
}
