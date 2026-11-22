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
import org.apache.seatunnel.engine.imap.storage.api.ExpirableValue;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
public class WALReaderAndWriterTest {

    private static FileSystem FS;
    private static final Path PARENT_PATH = new Path("/tmp/9/");
    private static final Path SAME_TIMESTAMP_TOMBSTONE_PATH =
            new Path("/tmp/imap-wal-same-timestamp-tombstone/");
    // A real pre-#9689 WAL file is not required to exercise the legacy path: the
    // relocated types (JobStatus, JobResult, JobStatusData) kept identical field
    // layout and enum ordinals across the #9689 move, and the static serialVersionUID
    // added later is skipped by ProtoStuff's RuntimeSchema, so the only wire difference
    // is the valueClassName string, which these fixtures set to the old names explicitly.
    // Replaying a captured pre-relocation WAL frame remains a welcome follow-up.
    private static final Path LEGACY_JOB_STATUS_PATH = new Path("/tmp/imap-wal-legacy-job-status/");
    private static final Path UNKNOWN_CLASS_NAME_PATH =
            new Path("/tmp/imap-wal-unknown-class-name/");
    private static final Serializer SERIALIZER = new ProtoStuffSerializer();

    @BeforeAll
    public static void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FS = FileSystem.getLocal(conf);
    }

    @BeforeEach
    void cleanWalDirectory() throws IOException {
        FS.delete(PARENT_PATH, true);
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
    void shouldChooseLatestMutationsAcrossUnsortedFiles() throws IOException {
        Path path = new Path(PARENT_PATH, "unsorted");
        writeWalFile(
                new Path(path, "region-a/older-wal.txt"),
                mutation("updated", "old", 10, false),
                mutation("deleted", "old", 10, false));
        writeWalFile(
                new Path(path, "region-b/newer-wal.txt"),
                mutation("updated", "new", 20, false),
                mutation("deleted", null, 20, true));

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, Object> result = reader.loadAllData(path, Collections.emptySet());
        Set<Object> keys = reader.loadAllKeys(path);

        Assertions.assertEquals(Collections.singletonMap("updated", "new"), result);
        Assertions.assertEquals(Collections.singleton("updated"), keys);
    }

    @Test
    void shouldRetainOnlyRequestedKeysWhileScanning() throws IOException {
        Path path = new Path(PARENT_PATH, "selective");
        writeWalFile(
                new Path(path, "region/selective-wal.txt"),
                mutation("key-a", "value-a", 10, false),
                mutation("key-b", "value-b", 20, false),
                mutation("key-c", "value-c", 30, false));

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, Object> result =
                reader.loadAllData(path, new HashSet<>(Arrays.asList("key-a", "missing")));

        Assertions.assertEquals(Collections.singletonMap("key-a", "value-a"), result);
    }

    @Test
    void shouldIgnoreIncompleteTrailingRecord() throws IOException {
        Path path = new Path(PARENT_PATH, "partial");
        Path file = new Path(path, "region/partial-wal.txt");
        FS.mkdirs(file.getParent());
        byte[] complete = wrapped(mutation("complete", "value", 10, false));
        byte[] incomplete = wrapped(mutation("partial", "ignored", 20, false));
        try (FSDataOutputStream output = FS.create(file, true)) {
            output.write(complete);
            output.write(incomplete, 0, WALDataUtils.WAL_DATA_METADATA_LENGTH + 3);
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);

        Assertions.assertEquals(
                Collections.singletonMap("complete", "value"),
                reader.loadAllData(path, Collections.emptySet()));
    }

    @Test
    void shouldSkipExpiredEntriesAndReturnExpirationTimes() throws IOException {
        Path path = new Path(PARENT_PATH, "expiration");
        long now = System.currentTimeMillis();
        long liveExpirationTime = now + TimeUnit.MINUTES.toMillis(10);
        IMapFileData expired = mutation("expired", "value-a", 10, false);
        expired.setExpirationTime(now - 1);
        IMapFileData live = mutation("live", "value-b", 10, false);
        live.setExpirationTime(liveExpirationTime);
        // Records written before the expiration time was stored have none.
        IMapFileData neverExpiring = mutation("never-expiring", "value-c", 10, false);
        writeWalFile(new Path(path, "region/expiration-wal.txt"), expired, live, neverExpiring);

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("live", "never-expiring")), reader.loadAllKeys(path));
        Map<Object, Object> expectedData = new HashMap<>();
        expectedData.put("live", "value-b");
        expectedData.put("never-expiring", "value-c");
        Assertions.assertEquals(expectedData, reader.loadAllData(path, Collections.emptySet()));
        Map<Object, ExpirableValue> expectedDataWithExpirationTime = new HashMap<>();
        expectedDataWithExpirationTime.put(
                "live", new ExpirableValue("value-b", liveExpirationTime));
        expectedDataWithExpirationTime.put("never-expiring", new ExpirableValue("value-c"));
        Assertions.assertEquals(
                expectedDataWithExpirationTime,
                reader.loadAllDataWithExpirationTime(path, Collections.emptySet()));
    }

    @Test
    void shouldNotFallBackToOlderValueWhenLatestValueExpired() throws IOException {
        Path path = new Path(PARENT_PATH, "latest-expired");
        IMapFileData expired = mutation("key", "new", 20, false);
        expired.setExpirationTime(System.currentTimeMillis() - 1);
        writeWalFile(
                new Path(path, "region/latest-expired-wal.txt"),
                mutation("key", "old", 10, false),
                expired);

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);

        Assertions.assertEquals(
                Collections.emptyMap(), reader.loadAllData(path, Collections.emptySet()));
        Assertions.assertEquals(Collections.emptySet(), reader.loadAllKeys(path));
    }

    @Test
    void shouldLoadRecordsWrittenBeforeExpirationTimeWasStored() throws IOException {
        Path path = new Path(PARENT_PATH, "pre-expiration-time");
        Path file = new Path(path, "region/pre-expiration-time-wal.txt");
        FS.mkdirs(file.getParent());
        try (FSDataOutputStream output = FS.create(file, true)) {
            output.write(
                    WALDataUtils.wrapperBytes(
                            SERIALIZER.serialize(preExpirationTimeRecord("key", "value", 10))));
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);

        Assertions.assertEquals(
                Collections.singletonMap("key", new ExpirableValue("value")),
                reader.loadAllDataWithExpirationTime(path, Collections.emptySet()));
    }

    @Test
    void shouldWriteRecordsWithoutExpirationTimeInThePreviousFormat() throws IOException {
        Assertions.assertArrayEquals(
                SERIALIZER.serialize(preExpirationTimeRecord("key", "value", 10)),
                SERIALIZER.serialize(mutation("key", "value", 10, false)));
    }

    @Test
    void shouldKeepRecordsWithExpirationTimeReadableByPreviousReleases() throws IOException {
        IMapFileData record = mutation("key", "value", 10, false);
        record.setExpirationTime(1_000L);

        PreExpirationTimeIMapFileData read =
                SERIALIZER.deserialize(
                        SERIALIZER.serialize(record), PreExpirationTimeIMapFileData.class);

        Assertions.assertFalse(read.deleted);
        Assertions.assertArrayEquals(record.getKey(), read.key);
        Assertions.assertEquals(record.getKeyClassName(), read.keyClassName);
        Assertions.assertArrayEquals(record.getValue(), read.value);
        Assertions.assertEquals(record.getValueClassName(), read.valueClassName);
        Assertions.assertEquals(record.getTimestamp(), read.timestamp);
    }

    private static IMapFileData mutation(String key, String value, long timestamp, boolean deleted)
            throws IOException {
        return IMapFileData.builder()
                .key(SERIALIZER.serialize(key))
                .keyClassName(String.class.getName())
                .value(deleted ? null : SERIALIZER.serialize(value))
                .valueClassName(deleted ? null : String.class.getName())
                .timestamp(timestamp)
                .deleted(deleted)
                .build();
    }

    private static void writeWalFile(Path file, IMapFileData... mutations) throws IOException {
        FS.mkdirs(file.getParent());
        try (FSDataOutputStream output = FS.create(file, true)) {
            for (IMapFileData mutation : mutations) {
                output.write(wrapped(mutation));
            }
        }
    }

    private static byte[] wrapped(IMapFileData mutation) throws IOException {
        return WALDataUtils.wrapperBytes(SERIALIZER.serialize(mutation));
    }

    private static PreExpirationTimeIMapFileData preExpirationTimeRecord(
            String key, String value, long timestamp) throws IOException {
        PreExpirationTimeIMapFileData record = new PreExpirationTimeIMapFileData();
        record.key = SERIALIZER.serialize(key);
        record.keyClassName = String.class.getName();
        record.value = SERIALIZER.serialize(value);
        record.valueClassName = String.class.getName();
        record.timestamp = timestamp;
        return record;
    }

    /**
     * Field layout of {@link IMapFileData} before {@code expirationTime} was added. ProtoStuff
     * numbers fields by declaration order, so this class reads and writes the same bytes as earlier
     * releases did.
     */
    public static class PreExpirationTimeIMapFileData {
        private boolean deleted;
        private byte[] key;
        private String keyClassName;
        private byte[] value;
        private String valueClassName;
        private long timestamp;
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

    /**
     * Binds every legacy mapping target to the live engine-common class at compile time: if a
     * target class is renamed again, this test fails to compile or fails the assertion instead of
     * leaving a stale shim behind.
     */
    @Test
    public void testLegacyMappingsTargetCurrentClassNames() throws Exception {
        java.lang.reflect.Field field =
                WALReader.class.getDeclaredField("LEGACY_CLASS_NAME_MAPPINGS");
        field.setAccessible(true);
        java.util.Map<String, String> mappings = (java.util.Map<String, String>) field.get(null);

        Assertions.assertEquals(
                JobStatus.class.getName(),
                mappings.get("org.apache.seatunnel.engine.core.job.JobStatus"));
        Assertions.assertEquals(
                JobResult.class.getName(),
                mappings.get("org.apache.seatunnel.engine.core.job.JobResult"));
        Assertions.assertEquals(
                JobStatusData.class.getName(),
                mappings.get("org.apache.seatunnel.engine.core.job.JobStatusData"));
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

    /**
     * Feeds a record whose class name is neither a current class nor a legacy mapping key: the
     * failure must surface the recorded and the resolved class name so a stale or wrong mapping is
     * diagnosable from the restart error alone. A name absent from the legacy table resolves to
     * itself, so both rendered names equal the recorded one.
     */
    @Test
    public void testUnknownClassNameSurfacesInExceptionMessage() throws Exception {
        String unknownClassName = "org.apache.seatunnel.engine.core.job.NeverExistedJobStatus";
        try (WALWriter writer =
                new WALWriter(FS, FileConfiguration.HDFS, UNKNOWN_CLASS_NAME_PATH, SERIALIZER)) {
            writer.write(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize("unknown-class"))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize("value"))
                            .valueClassName(unknownClassName)
                            .deleted(false)
                            .timestamp(System.nanoTime())
                            .build());
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        IMapStorageException exception =
                Assertions.assertThrows(
                        IMapStorageException.class,
                        () -> reader.loadAllData(UNKNOWN_CLASS_NAME_PATH, new HashSet<>()));

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "class name is "
                                        + unknownClassName
                                        + ", resolved class name is "
                                        + unknownClassName),
                "exception message should render the recorded and resolved class names, got: "
                        + exception.getMessage());
    }

    @AfterAll
    public static void close() throws IOException {
        FS.delete(PARENT_PATH, true);
        FS.delete(SAME_TIMESTAMP_TOMBSTONE_PATH, true);
        FS.delete(LEGACY_JOB_STATUS_PATH, true);
        FS.delete(UNKNOWN_CLASS_NAME_PATH, true);
        FS.close();
    }
}
