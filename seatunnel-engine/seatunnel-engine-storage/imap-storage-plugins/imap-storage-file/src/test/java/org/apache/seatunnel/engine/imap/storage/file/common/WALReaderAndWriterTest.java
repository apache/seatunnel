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
import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionPlan;
import org.apache.seatunnel.engine.imap.storage.file.compaction.HdfsCompactionStorage;
import org.apache.seatunnel.engine.imap.storage.file.compaction.RegionCompactionPlanner;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.DefaultReader;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.SegmentedHdfsWriter;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Test
    void shouldCompactRawMutationsWithoutResurrectingOtherRegionValues() throws IOException {
        Path root = new Path(PARENT_PATH, "compaction-metadata");
        Path scope = new Path(root, "region-b");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        writeWalFile(
                new Path(root, "region-a/legacy-wal.txt"),
                mutation("deleted", "old-region-value", 5, false),
                mutation("expired", "old-region-value", 5, false));
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        writeWalFile(
                first,
                mutation("updated", "old", 10, false),
                mutation("deleted", "old", 10, false),
                mutation("expired", "old", 10, false),
                mutation("expiring", "old", 10, false));
        IMapFileData updated = mutation("updated", "new", 20, false);
        IMapFileData deleted = mutation("deleted", null, 20, true);
        IMapFileData expired = mutation("expired", "expired-payload", 20, false);
        expired.setExpirationTime(System.currentTimeMillis() - 1);
        IMapFileData expiring = mutation("expiring", "live-payload", 20, false);
        expiring.setExpirationTime(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
        writeWalFile(second, updated, deleted, expired, expiring);
        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, ExpirableValue> before =
                reader.loadAllDataWithExpirationTime(root, Collections.emptySet());
        String previousToken =
                HdfsCompactionStorage.readToken(
                        FS, new Path(scope, HdfsCompactionStorage.CURRENT_FILE));

        Path snapshot =
                new WALCompactor()
                        .compact(new CompactionPlan(Arrays.asList(first, second), scope), storage);

        Map<String, IMapFileData> compacted = readRawMutationMap(snapshot);
        Assertions.assertEquals(4, compacted.size());
        for (IMapFileData expected : Arrays.asList(updated, deleted, expired, expiring)) {
            String key = SERIALIZER.deserialize(expected.getKey(), String.class);
            Assertions.assertArrayEquals(
                    SERIALIZER.serialize(expected), SERIALIZER.serialize(compacted.get(key)));
        }
        Assertions.assertNotEquals(
                previousToken,
                HdfsCompactionStorage.readToken(
                        FS, new Path(scope, HdfsCompactionStorage.CURRENT_FILE)));
        // A crash after publication may leave both inputs, or only one of them, alongside the
        // snapshot.
        Assertions.assertEquals(
                before, reader.loadAllDataWithExpirationTime(root, Collections.emptySet()));
        storage.delete(first);
        Assertions.assertEquals(
                before, reader.loadAllDataWithExpirationTime(root, Collections.emptySet()));
        storage.delete(second);
        Assertions.assertEquals(
                before, reader.loadAllDataWithExpirationTime(root, Collections.emptySet()));
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("updated", "expiring")), reader.loadAllKeys(root));
        Assertions.assertTrue(FS.exists(new Path(root, "region-a/legacy-wal.txt")));
    }

    @Test
    void shouldRotateCompleteSegmentsAndCompactWithoutTouchingActiveTail() throws IOException {
        Path root = new Path(PARENT_PATH, "rotating-writer");
        Path scope = new Path(root, "region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        RegionCompactionPlanner planner = new RegionCompactionPlanner(scope);
        IMapFileData older = mutation("key", "old", 10, false);
        IMapFileData latest = mutation("key", "new", 20, false);
        long segmentSize = wrapped(older).length + wrapped(latest).length;
        try (SegmentedHdfsWriter writer = new SegmentedHdfsWriter(segmentSize, planner)) {
            writer.initialize(FS, scope, SERIALIZER);
            writer.write(older);
            Assertions.assertFalse(
                    planner.plan().isPresent(), "an open segment must never be selected");
            writer.write(latest);
            CompactionPlan plan = planner.plan().get();
            Assertions.assertEquals(1, plan.getInputs().size());
            writer.write(mutation("tail", "active", 30, false));
            Path activeTail = new Path(scope, "segment-00000000000000000002-wal.txt");
            Assertions.assertTrue(FS.exists(activeTail));
            Assertions.assertFalse(plan.getInputs().contains(activeTail));

            Path snapshot = new WALCompactor().compact(plan, storage);
            planner.committed(plan, snapshot);
            for (Path input : plan.getInputs()) {
                storage.delete(input);
            }

            Assertions.assertFalse(planner.plan().isPresent());
            Assertions.assertTrue(FS.exists(activeTail));
            Assertions.assertArrayEquals(
                    SERIALIZER.serialize(latest),
                    SERIALIZER.serialize(readRawMutationMap(snapshot).get("key")));
            writer.write(mutation("tail", "still-writable", 40, false));
            Map<Object, Object> expected = new HashMap<>();
            expected.put("key", "new");
            expected.put("tail", "still-writable");
            WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
            Assertions.assertEquals(expected, reader.loadAllData(root, Collections.emptySet()));
        }
    }

    @Test
    void shouldMergeAnExplicitInputSetAcrossRegionsWithoutResolvingPayloadClasses()
            throws IOException {
        Path root = new Path(PARENT_PATH, "cross-region-plan");
        Path first = new Path(root, "region-a/first-wal.txt");
        Path second = new Path(root, "region-b/second-wal.txt");
        Path output = new Path(root, "independent-output");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(output);
        IMapFileData latest = mutation("shared", "opaque-payload", 20, false);
        latest.setValueClassName("example.UnavailablePayloadClass");
        IMapFileData separate = mutation("separate", "value", 10, false);
        writeWalFile(first, mutation("shared", "old", 10, false), separate);
        writeWalFile(second, latest);

        Path snapshot =
                new WALCompactor()
                        .compact(new CompactionPlan(Arrays.asList(first, second), output), storage);

        Assertions.assertEquals(FS.makeQualified(output), FS.makeQualified(snapshot.getParent()));
        Map<String, IMapFileData> compacted = readRawMutationMap(snapshot);
        Assertions.assertEquals(2, compacted.size());
        Assertions.assertArrayEquals(
                SERIALIZER.serialize(latest), SERIALIZER.serialize(compacted.get("shared")));
        Assertions.assertArrayEquals(
                SERIALIZER.serialize(separate), SERIALIZER.serialize(compacted.get("separate")));
        Assertions.assertTrue(FS.exists(first));
        Assertions.assertTrue(FS.exists(second));
    }

    @Test
    void shouldIgnoreAnInterruptedSnapshotTemporaryFile() throws IOException {
        Path root = new Path(PARENT_PATH, "interrupted-snapshot");
        Path scope = new Path(root, "region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        writeWalFile(new Path(scope, "segment-1-wal.txt"), mutation("key", "value", 10, false));
        Path temporary = new Path(scope, ".snapshot-interrupted.tmp");
        byte[] incomplete = wrapped(mutation("uncommitted", "ignored", 20, false));
        try (FSDataOutputStream output = FS.create(temporary, false)) {
            output.write(incomplete, 0, WALDataUtils.WAL_DATA_METADATA_LENGTH + 3);
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);

        Assertions.assertEquals(
                Collections.singletonMap("key", "value"),
                reader.loadAllData(root, Collections.emptySet()));
        Assertions.assertEquals(Collections.singleton("key"), reader.loadAllKeys(root));
        Assertions.assertTrue(FS.exists(temporary));
    }

    @Test
    void shouldKeepInputsWhenSnapshotTokenPublicationFails() throws IOException {
        Path root = new Path(PARENT_PATH, "failed-publication");
        Path scope = new Path(root, "region");
        HdfsCompactionStorage storage =
                new HdfsCompactionStorage(FS, SERIALIZER) {
                    @Override
                    protected void replaceToken(Path source, Path target) throws IOException {
                        throw new IOException("simulated token publication failure");
                    }
                };
        storage.initialize(scope);
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        writeWalFile(first, mutation("key", "old", 10, false));
        writeWalFile(second, mutation("key", "new", 20, false));
        Path tokenPath = new Path(scope, HdfsCompactionStorage.CURRENT_FILE);
        String previousToken = HdfsCompactionStorage.readToken(FS, tokenPath);

        for (int attempt = 0; attempt < 2; attempt++) {
            IOException exception =
                    Assertions.assertThrows(
                            IOException.class,
                            () ->
                                    new WALCompactor()
                                            .compact(
                                                    new CompactionPlan(
                                                            Arrays.asList(first, second), scope),
                                                    storage));

            Assertions.assertTrue(
                    exception.getMessage().contains("simulated token publication failure"));
            Assertions.assertEquals(previousToken, HdfsCompactionStorage.readToken(FS, tokenPath));
            Assertions.assertTrue(FS.exists(first));
            Assertions.assertTrue(FS.exists(second));
            Assertions.assertTrue(
                    Arrays.stream(FS.listStatus(scope))
                            .noneMatch(file -> file.getPath().getName().contains("snapshot-")),
                    "Repeated failed attempts must not accumulate untracked snapshots");
        }
        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);
        Assertions.assertEquals(
                Collections.singletonMap("key", "new"),
                reader.loadAllData(root, Collections.emptySet()));
    }

    @Test
    void shouldRejectATruncatedCompletedInputBeforePublishingSnapshot() throws IOException {
        Path scope = new Path(PARENT_PATH, "truncated-compaction-input/region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        writeWalFile(first, mutation("key", "old", 10, false));
        byte[] complete = wrapped(mutation("key", "new", 20, false));
        byte[] incomplete = wrapped(mutation("missing", "ignored", 30, false));
        try (FSDataOutputStream output = FS.create(second, false)) {
            output.write(complete);
            output.write(incomplete, 0, WALDataUtils.WAL_DATA_METADATA_LENGTH + 3);
        }
        Path tokenPath = new Path(scope, HdfsCompactionStorage.CURRENT_FILE);
        String previousToken = HdfsCompactionStorage.readToken(FS, tokenPath);

        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                new WALCompactor()
                                        .compact(
                                                new CompactionPlan(
                                                        Arrays.asList(first, second), scope),
                                                storage));

        Assertions.assertTrue(exception.getMessage().contains("Incomplete sealed WAL"));
        Assertions.assertEquals(previousToken, HdfsCompactionStorage.readToken(FS, tokenPath));
        Assertions.assertTrue(FS.exists(first));
        Assertions.assertTrue(FS.exists(second));
        Assertions.assertTrue(
                Arrays.stream(FS.listStatus(scope))
                        .noneMatch(
                                file ->
                                        file.getPath()
                                                .getName()
                                                .startsWith(
                                                        HdfsCompactionStorage.SNAPSHOT_PREFIX)));
    }

    @Test
    void shouldDeferAmbiguousExpirationTimesUntilANewerMutationResolvesThem() throws IOException {
        Path scope = new Path(PARENT_PATH, "ambiguous-expiration/region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        IMapFileData expiring = mutation("key", "same-value", 10, false);
        expiring.setExpirationTime(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
        writeWalFile(first, mutation("key", "same-value", 10, false));
        writeWalFile(second, expiring);
        Path tokenPath = new Path(scope, HdfsCompactionStorage.CURRENT_FILE);
        String previousToken = HdfsCompactionStorage.readToken(FS, tokenPath);

        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                new WALCompactor()
                                        .compact(
                                                new CompactionPlan(
                                                        Arrays.asList(first, second), scope),
                                                storage));

        Assertions.assertTrue(exception.getMessage().contains("different expiration times"));
        Assertions.assertEquals(previousToken, HdfsCompactionStorage.readToken(FS, tokenPath));
        Assertions.assertTrue(FS.exists(first));
        Assertions.assertTrue(FS.exists(second));
        Path third = new Path(scope, "segment-3-wal.txt");
        IMapFileData latest = mutation("key", "same-value", 20, false);
        writeWalFile(third, latest);
        Path snapshot =
                new WALCompactor()
                        .compact(
                                new CompactionPlan(Arrays.asList(first, second, third), scope),
                                storage);
        Assertions.assertArrayEquals(
                SERIALIZER.serialize(latest),
                SERIALIZER.serialize(readRawMutationMap(snapshot).get("key")));
    }

    @Test
    void shouldRetryWhenCompactionCommitsAfterInventoryWithoutDeletingInputs() throws IOException {
        Path root = new Path(PARENT_PATH, "concurrent-publication");
        Path scope = new Path(root, "region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        writeWalFile(first, mutation("key", "old", 10, false));
        writeWalFile(second, mutation("key", "new", 20, false));
        AtomicBoolean commitOnce = new AtomicBoolean();
        AtomicInteger firstInputReads = new AtomicInteger();
        FileSystem racingFileSystem =
                new FilterFileSystem(FS) {
                    @Override
                    public FSDataInputStream open(Path file, int bufferSize) throws IOException {
                        if (file.equals(FS.makeQualified(first))) {
                            firstInputReads.incrementAndGet();
                            if (commitOnce.compareAndSet(false, true)) {
                                new WALCompactor()
                                        .compact(
                                                new CompactionPlan(
                                                        Arrays.asList(first, second), scope),
                                                storage);
                            }
                        }
                        return super.open(file, bufferSize);
                    }
                };
        WALReader reader = new WALReader(racingFileSystem, FileConfiguration.HDFS, SERIALIZER);

        Assertions.assertEquals(
                Collections.singletonMap("key", "new"),
                reader.loadAllData(root, Collections.emptySet()));
        Assertions.assertTrue(commitOnce.get());
        Assertions.assertEquals(
                2, firstInputReads.get(), "the changed token must force a fresh scan");
        Assertions.assertTrue(FS.exists(first));
        Assertions.assertTrue(FS.exists(second));
    }

    @Test
    void shouldRetryWhenListedInputsDisappearDuringCompaction() throws IOException {
        Path root = new Path(PARENT_PATH, "concurrent-retirement");
        Path scope = new Path(root, "region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        writeWalFile(first, mutation("key", "old", 10, false));
        writeWalFile(second, mutation("key", "new", 20, false));
        AtomicBoolean compactOnce = new AtomicBoolean();
        FileSystem racingFileSystem =
                new FilterFileSystem(FS) {
                    @Override
                    public FSDataInputStream open(Path file, int bufferSize) throws IOException {
                        if (file.equals(FS.makeQualified(first))
                                && compactOnce.compareAndSet(false, true)) {
                            new WALCompactor()
                                    .compact(
                                            new CompactionPlan(Arrays.asList(first, second), scope),
                                            storage);
                            storage.delete(first);
                            storage.delete(second);
                        }
                        return super.open(file, bufferSize);
                    }
                };
        WALReader reader = new WALReader(racingFileSystem, FileConfiguration.HDFS, SERIALIZER);

        Assertions.assertEquals(
                Collections.singletonMap("key", "new"),
                reader.loadAllData(root, Collections.emptySet()));
        Assertions.assertTrue(compactOnce.get());
        Assertions.assertFalse(FS.exists(first));
        Assertions.assertFalse(FS.exists(second));
    }

    @Test
    void shouldRetryWhenPagedInventoryMissesBothRetiredInputsAndTheirSnapshot() throws IOException {
        Path root = new Path(PARENT_PATH, "concurrent-paged-inventory");
        Path scope = new Path(root, "region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        Path tail = new Path(scope, "segment-3-wal.txt");
        writeWalFile(first, mutation("compacted", "old", 10, false));
        writeWalFile(second, mutation("compacted", "new", 20, false));
        writeWalFile(tail, mutation("tail", "still-active", 30, false));
        Set<Path> retiredInputs =
                new HashSet<>(Arrays.asList(FS.makeQualified(first), FS.makeQualified(second)));
        AtomicInteger inventories = new AtomicInteger();
        FileSystem racingFileSystem =
                new FilterFileSystem(FS) {
                    @Override
                    public RemoteIterator<LocatedFileStatus> listFiles(Path path, boolean recursive)
                            throws IOException {
                        RemoteIterator<LocatedFileStatus> original =
                                super.listFiles(path, recursive);
                        if (inventories.incrementAndGet() != 2) {
                            return original;
                        }
                        List<LocatedFileStatus> staleInventory = new ArrayList<>();
                        while (original.hasNext()) {
                            LocatedFileStatus file = original.next();
                            if (!retiredInputs.contains(file.getPath())) {
                                staleInventory.add(file);
                            }
                        }
                        // The new snapshot falls behind a paged listing's cursor; the inputs
                        // disappear before its remaining pages, so neither representation is
                        // listed.
                        new WALCompactor()
                                .compact(
                                        new CompactionPlan(Arrays.asList(first, second), scope),
                                        storage);
                        storage.delete(first);
                        storage.delete(second);
                        Iterator<LocatedFileStatus> files = staleInventory.iterator();
                        return new RemoteIterator<LocatedFileStatus>() {
                            @Override
                            public boolean hasNext() {
                                return files.hasNext();
                            }

                            @Override
                            public LocatedFileStatus next() {
                                return files.next();
                            }
                        };
                    }
                };
        WALReader reader = new WALReader(racingFileSystem, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, Object> expected = new HashMap<>();
        expected.put("compacted", "new");
        expected.put("tail", "still-active");

        Assertions.assertEquals(expected, reader.loadAllData(root, Collections.emptySet()));
        Assertions.assertTrue(
                inventories.get() >= 4, "replay must discard the incomplete inventory");
        Assertions.assertFalse(FS.exists(first));
        Assertions.assertFalse(FS.exists(second));
        Assertions.assertTrue(FS.exists(tail));
    }

    @Test
    void shouldDiscoverATokenOmittedByTheInitialScopeInventory() throws IOException {
        Path root = new Path(PARENT_PATH, "omitted-new-scope-token");
        Path scope = new Path(root, "region");
        HdfsCompactionStorage storage = new HdfsCompactionStorage(FS, SERIALIZER);
        storage.initialize(scope);
        Path first = new Path(scope, "segment-1-wal.txt");
        Path second = new Path(scope, "segment-2-wal.txt");
        Path tail = new Path(scope, "segment-3-wal.txt");
        writeWalFile(first, mutation("compacted", "old", 10, false));
        writeWalFile(second, mutation("compacted", "new", 20, false));
        writeWalFile(tail, mutation("tail", "still-active", 30, false));
        Set<Path> retiredInputs =
                new HashSet<>(Arrays.asList(FS.makeQualified(first), FS.makeQualified(second)));
        AtomicInteger inventories = new AtomicInteger();
        AtomicInteger tokenReadsBeforeDataInventory = new AtomicInteger();
        FileSystem racingFileSystem =
                new FilterFileSystem(FS) {
                    @Override
                    public FSDataInputStream open(Path file, int bufferSize) throws IOException {
                        if (file.getName().equals(HdfsCompactionStorage.CURRENT_FILE)
                                && inventories.get() == 1) {
                            tokenReadsBeforeDataInventory.incrementAndGet();
                        }
                        return super.open(file, bufferSize);
                    }

                    @Override
                    public RemoteIterator<LocatedFileStatus> listFiles(Path path, boolean recursive)
                            throws IOException {
                        RemoteIterator<LocatedFileStatus> original =
                                super.listFiles(path, recursive);
                        int inventory = inventories.incrementAndGet();
                        if (inventory > 2) {
                            return original;
                        }
                        List<LocatedFileStatus> partialInventory = new ArrayList<>();
                        while (original.hasNext()) {
                            LocatedFileStatus file = original.next();
                            if (inventory == 1
                                    && file.getPath()
                                            .getName()
                                            .equals(HdfsCompactionStorage.CURRENT_FILE)) {
                                // The token was created behind the cursor, but its segments
                                // were created later and still appear in this initial listing.
                                continue;
                            }
                            if (inventory != 2 || !retiredInputs.contains(file.getPath())) {
                                partialInventory.add(file);
                            }
                        }
                        if (inventory == 2) {
                            new WALCompactor()
                                    .compact(
                                            new CompactionPlan(Arrays.asList(first, second), scope),
                                            storage);
                            storage.delete(first);
                            storage.delete(second);
                        }
                        Iterator<LocatedFileStatus> files = partialInventory.iterator();
                        return new RemoteIterator<LocatedFileStatus>() {
                            @Override
                            public boolean hasNext() {
                                return files.hasNext();
                            }

                            @Override
                            public LocatedFileStatus next() {
                                return files.next();
                            }
                        };
                    }
                };
        WALReader reader = new WALReader(racingFileSystem, FileConfiguration.HDFS, SERIALIZER);
        Map<Object, Object> expected = new HashMap<>();
        expected.put("compacted", "new");
        expected.put("tail", "still-active");

        Assertions.assertEquals(expected, reader.loadAllData(root, Collections.emptySet()));
        Assertions.assertEquals(
                1,
                tokenReadsBeforeDataInventory.get(),
                "a segment path must trigger token discovery before a fresh data inventory");
        Assertions.assertTrue(
                inventories.get() >= 4, "the discovered token must detect and retry the commit");
        Assertions.assertFalse(FS.exists(first));
        Assertions.assertFalse(FS.exists(second));
        Assertions.assertTrue(FS.exists(tail));
    }

    private static Map<String, IMapFileData> readRawMutationMap(Path snapshot) throws IOException {
        Map<String, IMapFileData> records = new HashMap<>();
        DefaultReader reader = new DefaultReader();
        reader.initialize(FS, SERIALIZER);
        reader.readCompletedFile(
                snapshot,
                mutation -> {
                    String key = SERIALIZER.deserialize(mutation.getKey(), String.class);
                    Assertions.assertNull(
                            records.put(key, mutation), "snapshot must keep one mutation per key");
                });
        return records;
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
