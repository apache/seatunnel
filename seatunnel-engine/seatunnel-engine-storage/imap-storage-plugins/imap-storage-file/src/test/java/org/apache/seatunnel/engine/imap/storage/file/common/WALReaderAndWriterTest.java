/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.JobStatusData;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
public class WALReaderAndWriterTest {

    private static FileSystem FS;
    private static final Path PARENT_PATH = new Path("/tmp/9/");
    private static final Path SAME_TIMESTAMP_TOMBSTONE_PATH =
            new Path("/tmp/imap-wal-same-timestamp-tombstone/");
    private static final Path LEGACY_JOB_STATUS_PATH = new Path("/tmp/imap-wal-legacy-job-status/");
    private static final Serializer SERIALIZER = new ProtoStuffSerializer();

    @BeforeAll
    public static void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FS = FileSystem.getLocal(conf);
    }

    @Test
    public void testWriterAndReader() throws Exception {
        WALWriter writer = new WALWriter(FS, FileConfiguration.HDFS, PARENT_PATH, SERIALIZER);
        IMapFileData data;
        boolean isDelete;
        for (int i = 0; i < 1024; i++) {
            data =
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize("key" + i))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize("value" + i))
                            .valueClassName(Integer.class.getName())
                            .timestamp(System.nanoTime())
                            .build();
            if (i % 2 == 0) {
                isDelete = true;
                data.setKey(SERIALIZER.serialize(i));
                data.setKeyClassName(Integer.class.getName());
            } else {
                isDelete = false;
            }
            data.setDeleted(isDelete);

            writer.write(data);
        }
        // update key 511
        data =
                IMapFileData.builder()
                        .key(SERIALIZER.serialize("key" + 511))
                        .keyClassName(String.class.getName())
                        .value(SERIALIZER.serialize("Kristen"))
                        .valueClassName(String.class.getName())
                        .deleted(false)
                        .timestamp(System.nanoTime())
                        .build();
        writer.write(data);
        // delete key 519
        data =
                IMapFileData.builder()
                        .key(SERIALIZER.serialize("key" + 519))
                        .keyClassName(String.class.getName())
                        .deleted(true)
                        .timestamp(System.nanoTime())
                        .build();

        writer.write(data);
        writer.close();
        await().atMost(10, java.util.concurrent.TimeUnit.SECONDS).await();

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, new ProtoStuffSerializer());
        Map<Object, Object> result = reader.loadAllData(PARENT_PATH, new HashSet<>());
        Assertions.assertEquals("Kristen", result.get("key511"));
        Assertions.assertEquals(511, result.size());
        Assertions.assertNull(result.get("key519"));
    }

    @Test
    public void testReplayKeepsTombstoneForSameKeyAndTimestamp() throws Exception {
        String key = "deleted-key";
        long timestamp = 1000L;

        try (WALWriter writer =
                new WALWriter(
                        FS, FileConfiguration.HDFS, SAME_TIMESTAMP_TOMBSTONE_PATH, SERIALIZER)) {
            writer.write(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(key))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize("value"))
                            .valueClassName(String.class.getName())
                            .deleted(false)
                            .timestamp(timestamp)
                            .build());
            writer.write(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(key))
                            .keyClassName(String.class.getName())
                            .deleted(true)
                            .timestamp(timestamp)
                            .build());
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, Object> data =
                reader.loadAllData(SAME_TIMESTAMP_TOMBSTONE_PATH, new HashSet<>());

        Assertions.assertFalse(data.containsKey(key));
        Assertions.assertFalse(reader.loadAllKeys(SAME_TIMESTAMP_TOMBSTONE_PATH).contains(key));
    }

    @Test
    public void testReaderLoadsJobStatusWrittenWithLegacyClassName() throws Exception {
        String key = "job-status";
        try (WALWriter writer =
                new WALWriter(FS, FileConfiguration.HDFS, LEGACY_JOB_STATUS_PATH, SERIALIZER)) {
            writer.write(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(key))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize(JobStatus.RUNNING))
                            .valueClassName("org.apache.seatunnel.engine.core.job.JobStatus")
                            .deleted(false)
                            .timestamp(System.nanoTime())
                            .build());
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, Object> data = reader.loadAllData(LEGACY_JOB_STATUS_PATH, new HashSet<>());

        Assertions.assertEquals(JobStatus.class, data.get(key).getClass());
        // ProtoStuff returns a non-canonical enum instance, so compare by value, not identity.
        Assertions.assertEquals(JobStatus.RUNNING.name(), ((JobStatus) data.get(key)).name());
    }

    @Test
    public void testReaderLoadsAllLegacyJobModelClassNames() throws Exception {
        String jobStatusKey = "legacy-job-status";
        String jobResultKey = "legacy-job-result";
        String jobStatusDataKey = "legacy-job-status-data";
        JobStatusData jobStatusData =
                new JobStatusData(1L, "legacy-job", JobStatus.FINISHED, 1L, 2L, 3L);
        JobResult jobResult = new JobResult(JobStatus.CANCELED, "legacy error");
        try (WALWriter writer =
                new WALWriter(FS, FileConfiguration.HDFS, LEGACY_JOB_STATUS_PATH, SERIALIZER)) {
            writer.write(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(jobStatusKey))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize(JobStatus.RUNNING))
                            .valueClassName("org.apache.seatunnel.engine.core.job.JobStatus")
                            .deleted(false)
                            .timestamp(System.nanoTime())
                            .build());
            writer.write(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(jobResultKey))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize(jobResult))
                            .valueClassName("org.apache.seatunnel.engine.core.job.JobResult")
                            .deleted(false)
                            .timestamp(System.nanoTime())
                            .build());
            writer.write(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(jobStatusDataKey))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize(jobStatusData))
                            .valueClassName("org.apache.seatunnel.engine.core.job.JobStatusData")
                            .deleted(false)
                            .timestamp(System.nanoTime())
                            .build());
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, Object> data = reader.loadAllData(LEGACY_JOB_STATUS_PATH, new HashSet<>());

        Assertions.assertEquals(
                JobStatus.RUNNING.name(), ((JobStatus) data.get(jobStatusKey)).name());

        JobResult replayedResult = (JobResult) data.get(jobResultKey);
        Assertions.assertEquals(JobResult.class, replayedResult.getClass());
        Assertions.assertEquals(JobStatus.CANCELED.name(), replayedResult.getStatus().name());
        Assertions.assertEquals("legacy error", replayedResult.getError());

        JobStatusData replayedStatusData = (JobStatusData) data.get(jobStatusDataKey);
        Assertions.assertEquals(JobStatusData.class, replayedStatusData.getClass());
        Assertions.assertEquals(jobStatusData.getJobId(), replayedStatusData.getJobId());
        Assertions.assertEquals(jobStatusData.getJobName(), replayedStatusData.getJobName());
        Assertions.assertEquals(
                jobStatusData.getJobStatus().name(), replayedStatusData.getJobStatus().name());
        Assertions.assertEquals(jobStatusData.getSubmitTime(), replayedStatusData.getSubmitTime());
        Assertions.assertEquals(jobStatusData.getStartTime(), replayedStatusData.getStartTime());
        Assertions.assertEquals(jobStatusData.getFinishTime(), replayedStatusData.getFinishTime());
    }

    @AfterAll
    public static void close() throws IOException {
        FS.delete(PARENT_PATH, true);
        FS.delete(SAME_TIMESTAMP_TOMBSTONE_PATH, true);
        FS.delete(LEGACY_JOB_STATUS_PATH, true);
        FS.close();
    }
}
