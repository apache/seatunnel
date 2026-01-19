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

package org.apache.seatunnel.connectors.seatunnel.hive.commit;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class HiveSinkAggregatedCommitterOverwriteStreamingTest {

    private static class TestableCommitter extends HiveSinkAggregatedCommitter {
        TestableCommitter(
                ReadonlyConfig cfg, String dbName, String tableName, HadoopConf hadoopConf) {
            super(cfg, dbName, tableName, hadoopConf);
        }

        void setFileSystemProxy(HadoopFileSystemProxy proxy) {
            this.hadoopFileSystemProxy = proxy;
        }
    }

    @Test
    void shouldDeletePartitionDirectoryOnlyOnceAcrossStreamingCheckpoints() throws Exception {
        // Given
        ReadonlyConfig readonlyConfig = minimalHiveReadonlyConfig(true);
        TestableCommitter committer =
                new TestableCommitter(readonlyConfig, "db", "tbl", new HadoopConf("hdfs://dummy"));

        HiveMetaStoreCatalog hiveMetaStore = Mockito.mock(HiveMetaStoreCatalog.class);
        Mockito.doNothing()
                .when(hiveMetaStore)
                .addPartitions(Mockito.anyString(), Mockito.anyString(), Mockito.anyList());
        setHiveMetaStore(committer, hiveMetaStore);

        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        committer.setFileSystemProxy(fs);

        String partitionDir = "/warehouse/db/tbl/pt=2025-12-16";

        // checkpoint 1: empty transaction (matches production log pattern)
        FileAggregatedCommitInfo cp1Empty =
                aggregatedCommitInfo(
                        "/tmp/seatunnel/T_job_0_1", Collections.emptyMap(), Collections.emptyMap());

        // checkpoint 2: has one file -> should trigger overwrite deletion once
        FileAggregatedCommitInfo cp2 =
                aggregatedCommitInfo(
                        "/tmp/seatunnel/T_job_0_2",
                        Collections.singletonMap(
                                "/tmp/seatunnel/T_job_0_2/pt=2025-12-16/f1.parquet",
                                partitionDir + "/f1.parquet"),
                        Collections.singletonMap(
                                "pt=2025-12-16", Collections.singletonList("2025-12-16")));

        // checkpoint 3: has one more file -> MUST NOT delete partitionDir again
        FileAggregatedCommitInfo cp3 =
                aggregatedCommitInfo(
                        "/tmp/seatunnel/T_job_0_3",
                        Collections.singletonMap(
                                "/tmp/seatunnel/T_job_0_3/pt=2025-12-16/f2.parquet",
                                partitionDir + "/f2.parquet"),
                        Collections.singletonMap(
                                "pt=2025-12-16", Collections.singletonList("2025-12-16")));

        // When
        committer.commit(Collections.singletonList(cp1Empty));
        committer.commit(Collections.singletonList(cp2));
        committer.commit(Collections.singletonList(cp3));

        // Then
        // deleteFile is also used to delete transaction dirs in super.commit(). We only assert
        // deletion of the *target* partition directory happens once.
        Mockito.verify(fs, Mockito.times(1)).deleteFile(partitionDir);
    }

    @Test
    void shouldDeleteEachNewPartitionDirectoryOnlyOnceAcrossStreamingCheckpoints()
            throws Exception {
        // Given
        ReadonlyConfig readonlyConfig = minimalHiveReadonlyConfig(true);
        TestableCommitter committer =
                new TestableCommitter(readonlyConfig, "db", "tbl", new HadoopConf("hdfs://dummy"));

        HiveMetaStoreCatalog hiveMetaStore = Mockito.mock(HiveMetaStoreCatalog.class);
        Mockito.doNothing()
                .when(hiveMetaStore)
                .addPartitions(Mockito.anyString(), Mockito.anyString(), Mockito.anyList());
        setHiveMetaStore(committer, hiveMetaStore);

        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        committer.setFileSystemProxy(fs);

        String partitionDir1 = "/warehouse/db/tbl/pt=2025-12-16";
        String partitionDir2 = "/warehouse/db/tbl/pt=2025-12-17";

        // checkpoint 1: empty transaction
        FileAggregatedCommitInfo cp1Empty =
                aggregatedCommitInfo(
                        "/tmp/seatunnel/T_job_0_1", Collections.emptyMap(), Collections.emptyMap());

        // checkpoint 2: first partition
        FileAggregatedCommitInfo cp2 =
                aggregatedCommitInfo(
                        "/tmp/seatunnel/T_job_0_2",
                        Collections.singletonMap(
                                "/tmp/seatunnel/T_job_0_2/pt=2025-12-16/f1.parquet",
                                partitionDir1 + "/f1.parquet"),
                        Collections.singletonMap(
                                "pt=2025-12-16", Collections.singletonList("2025-12-16")));

        // checkpoint 3: new partition appears
        FileAggregatedCommitInfo cp3 =
                aggregatedCommitInfo(
                        "/tmp/seatunnel/T_job_0_3",
                        Collections.singletonMap(
                                "/tmp/seatunnel/T_job_0_3/pt=2025-12-17/f2.parquet",
                                partitionDir2 + "/f2.parquet"),
                        Collections.singletonMap(
                                "pt=2025-12-17", Collections.singletonList("2025-12-17")));

        // When
        committer.commit(Collections.singletonList(cp1Empty));
        committer.commit(Collections.singletonList(cp2));
        committer.commit(Collections.singletonList(cp3));

        // Then
        Mockito.verify(fs, Mockito.times(1)).deleteFile(partitionDir1);
        Mockito.verify(fs, Mockito.times(1)).deleteFile(partitionDir2);
    }

    @Test
    void e2eLikeCommitShouldAccumulateFilesAcrossCheckpointsWhenOverwriteEnabled(
            @TempDir Path tempDir) throws Exception {
        // Given
        ReadonlyConfig readonlyConfig = minimalHiveReadonlyConfig(true);
        TestableCommitter committer =
                new TestableCommitter(readonlyConfig, "db", "tbl", new HadoopConf("hdfs://dummy"));

        HiveMetaStoreCatalog hiveMetaStore = Mockito.mock(HiveMetaStoreCatalog.class);
        Mockito.doNothing()
                .when(hiveMetaStore)
                .addPartitions(Mockito.anyString(), Mockito.anyString(), Mockito.anyList());
        setHiveMetaStore(committer, hiveMetaStore);

        // Build a mock FS proxy that actually moves/deletes on local FS.
        HadoopFileSystemProxy fs = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.doAnswer(
                        invocation -> {
                            String oldPath = invocation.getArgument(0);
                            String newPath = invocation.getArgument(1);
                            boolean removeWhenExists = invocation.getArgument(2);

                            Path oldP = Paths.get(oldPath);
                            Path newP = Paths.get(newPath);

                            if (!Files.exists(oldP)) {
                                return null;
                            }

                            if (removeWhenExists && Files.exists(newP)) {
                                Files.delete(newP);
                            }
                            if (newP.getParent() != null) {
                                Files.createDirectories(newP.getParent());
                            }
                            Files.move(oldP, newP, StandardCopyOption.REPLACE_EXISTING);
                            return null;
                        })
                .when(fs)
                .renameFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean());

        Mockito.doAnswer(
                        invocation -> {
                            String pathStr = invocation.getArgument(0);
                            Path p = Paths.get(pathStr);
                            if (!Files.exists(p)) {
                                return null;
                            }
                            // delete recursively
                            try (Stream<Path> walk = Files.walk(p)) {
                                walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                                        .forEach(
                                                x -> {
                                                    try {
                                                        Files.deleteIfExists(x);
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                });
                            }
                            return null;
                        })
                .when(fs)
                .deleteFile(Mockito.anyString());

        committer.setFileSystemProxy(fs);

        Path targetPartitionDir = tempDir.resolve("warehouse/db/tbl/pt=2025-12-16");
        String partitionDir = targetPartitionDir.toString();

        // checkpoint 1: empty transaction
        FileAggregatedCommitInfo cp1Empty =
                aggregatedCommitInfo(
                        tempDir.resolve("txn/T_job_0_1").toString(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        // checkpoint 2: create a temp file to be moved
        Path txn2 = tempDir.resolve("txn/T_job_0_2");
        Path tmpFile1 = txn2.resolve("pt=2025-12-16/f1.parquet");
        Files.createDirectories(tmpFile1.getParent());
        Files.write(tmpFile1, "file1".getBytes(StandardCharsets.UTF_8));

        FileAggregatedCommitInfo cp2 =
                aggregatedCommitInfo(
                        txn2.toString(),
                        Collections.singletonMap(
                                tmpFile1.toString(),
                                targetPartitionDir.resolve("f1.parquet").toString()),
                        Collections.singletonMap(
                                "pt=2025-12-16", Collections.singletonList("2025-12-16")));

        // checkpoint 3: another temp file
        Path txn3 = tempDir.resolve("txn/T_job_0_3");
        Path tmpFile2 = txn3.resolve("pt=2025-12-16/f2.parquet");
        Files.createDirectories(tmpFile2.getParent());
        Files.write(tmpFile2, "file2".getBytes(StandardCharsets.UTF_8));

        FileAggregatedCommitInfo cp3 =
                aggregatedCommitInfo(
                        txn3.toString(),
                        Collections.singletonMap(
                                tmpFile2.toString(),
                                targetPartitionDir.resolve("f2.parquet").toString()),
                        Collections.singletonMap(
                                "pt=2025-12-16", Collections.singletonList("2025-12-16")));

        // When
        committer.commit(Collections.singletonList(cp1Empty));
        committer.commit(Collections.singletonList(cp2));
        committer.commit(Collections.singletonList(cp3));

        // Then
        Assertions.assertTrue(Files.isDirectory(targetPartitionDir));
        Assertions.assertTrue(Files.exists(targetPartitionDir.resolve("f1.parquet")));
        Assertions.assertTrue(Files.exists(targetPartitionDir.resolve("f2.parquet")));

        long fileCount;
        try (Stream<Path> stream = Files.list(targetPartitionDir)) {
            fileCount = stream.count();
        }
        Assertions.assertEquals(2, fileCount);

        // sanity: partition deletion should only happen once
        Mockito.verify(fs, Mockito.times(1)).deleteFile(partitionDir);
    }

    private static FileAggregatedCommitInfo aggregatedCommitInfo(
            String transactionDir,
            Map<String, String> fileMoves,
            Map<String, List<String>> partitions) {
        LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap = new LinkedHashMap<>();
        LinkedHashMap<String, String> moveMap = new LinkedHashMap<>();
        moveMap.putAll(fileMoves);
        transactionMap.put(transactionDir, moveMap);

        LinkedHashMap<String, List<String>> partitionMap = new LinkedHashMap<>();
        partitionMap.putAll(partitions);

        return new FileAggregatedCommitInfo(transactionMap, partitionMap);
    }

    private static ReadonlyConfig minimalHiveReadonlyConfig(boolean overwrite) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        // Required by HiveMetaStoreCatalog ctor
        map.put(HiveOptions.METASTORE_URI.key(), "thrift://dummy:9083");
        map.put(HiveConfig.HADOOP_CONF_PATH.key(), "/tmp");
        map.put(HiveConfig.HIVE_SITE_PATH.key(), "/tmp/hive-site.xml");

        // Used by HiveSinkAggregatedCommitter
        map.put(HiveSinkOptions.OVERWRITE.key(), overwrite);
        // other options are defaulted

        return ReadonlyConfig.fromMap(map);
    }

    private static void setHiveMetaStore(
            HiveSinkAggregatedCommitter committer, HiveMetaStoreCatalog hiveMetaStore)
            throws Exception {
        Field f = HiveSinkAggregatedCommitter.class.getDeclaredField("hiveMetaStore");
        f.setAccessible(true);
        f.set(committer, hiveMetaStore);
    }
}
