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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

/**
 * Regression coverage for the {@code IMapFileData}/{@code IMapData} {@link Comparable} contract
 * violation reported in apache/seatunnel#11462 and fixed by apache/seatunnel#11465.
 *
 * <p>Before the fix, {@code IMapFileData.compareTo} was implemented as {@code return o.timestamp -
 * this.timestamp > 0 ? 1 : -1;}. That expression never returns {@code 0}, so for two records
 * sharing the exact same {@code timestamp} (realistic under batch writes, since {@code
 * IMapFileStorage} stamps every record with {@code System.currentTimeMillis()}), both {@code
 * compare(a, b)} and {@code compare(b, a)} evaluated to {@code -1}. That violates {@code
 * Comparable}'s antisymmetry requirement {@code sgn(compare(x, y)) == -sgn(compare(y, x))} and had
 * two concrete production consequences:
 *
 * <ol>
 *   <li>{@code WALReader.loadAllData}/{@code loadAllKeys} call {@code Collections.sort} on every
 *       full read of the WAL. Once enough same-timestamp records are present to push the sort past
 *       TimSort's binary-insertion fast path (its {@code MIN_MERGE} threshold of 32 elements), the
 *       merge-invariant check throws {@code IllegalArgumentException: Comparison method violates
 *       its general contract!}, crashing IMap recovery.
 *   <li>Even when it does not throw, an antisymmetry-violating comparator leaves the relative order
 *       of tied records unspecified. {@code WALReader}'s replay loop treats the first record it
 *       encounters per key (after sorting) as authoritative: it either records the value or records
 *       a tombstone, then skips every later record for that key. If a same-millisecond delete for a
 *       key did not happen to sort ahead of a put for the same key, the delete was silently dropped
 *       and the key survived a delete that should have removed it.
 * </ol>
 *
 * <p>The fix (see {@code IMapDataComparator#compare}, shared by both {@code IMapFileData} and
 * {@code IMapData}) restores a real total order: key class name, then key bytes, then timestamp
 * descending, then -- only as a deterministic tie-breaker for genuine same-millisecond collisions
 * on the same key, since no portable atomic write-sequence primitive exists across the supported
 * Hadoop filesystems (HDFS, S3, OSS) -- deleted-before-not-deleted, then value class name, then
 * value bytes. Equal records now correctly compare as {@code 0}.
 *
 * <p>This class proves both consequences are closed, at a scale and through the exact call paths
 * the issue describes, without duplicating the direct comparator-unit coverage already added by the
 * fix ({@code IMapFileDataCompareToTest}) or the single-pair WAL replay coverage already added by
 * {@code WALReaderAndWriterTest#testReplayKeepsTombstoneForSameKeyAndTimestamp}:
 *
 * <ul>
 *   <li>{@link #testCollectionsSortOfLargeSameTimestampBatchSatisfiesComparableContract()} sorts a
 *       single 85-record, all-same-timestamp batch (40 independent same-key put/delete pairs plus 5
 *       uncontested control puts) and checks the contract holds across the whole batch at once,
 *       rather than for one isolated pair.
 *   <li>{@link #testWalReplayDoesNotLoseSameTimestampDeletesAcrossWriteOrders()} writes that same
 *       40-pair batch through the production {@code WALWriter}, deliberately alternating the
 *       physical write order per pair (put-before-delete for even-indexed keys, delete-before-put
 *       for odd-indexed keys), then replays it through the production {@code WALReader#loadAllData}
 *       and {@code #loadAllKeys} -- the exact methods the issue names as the crash site -- and
 *       asserts every delete is genuinely observed regardless of which order it was appended in,
 *       while the control keys survive untouched.
 * </ul>
 */
@EnabledOnOs({LINUX, MAC})
public class IMapFileDataTimestampCollisionRegressionTest {

    /**
     * Single timestamp shared by every record in the batch, so the comparator can never resolve a
     * pair by timestamp alone and must fall through to its deleted-before-not-deleted tie-breaker
     * for each same-key pair.
     */
    private static final long COLLISION_TIMESTAMP = 1000L;

    /**
     * Number of independent same-key put/delete pairs. 40 pairs plus the 5 control records below
     * give 85 total records, comfortably clearing TimSort's 32-element binary-insertion-sort
     * threshold so a real {@code Collections.sort} call exercises the merge path where the old,
     * contract-violating comparator could throw.
     */
    private static final int COLLISION_KEY_COUNT = 40;

    /**
     * Number of uncontested keys carrying only a put and no matching delete, used to prove the
     * large same-timestamp batch sort does not corrupt or drop unrelated data.
     */
    private static final int CONTROL_KEY_COUNT = 5;

    /**
     * Key prefix for the colliding put/delete pairs built by {@link #buildCollisionPairs()}; each
     * generated key is unique so that no two pairs can interfere with each other during the sort.
     */
    private static final String COLLISION_KEY_PREFIX = "collision-key-";

    /**
     * Key prefix for the uncontested control puts built by {@link #buildControlRecords()}; these
     * carry no matching delete, so they must always survive the same-timestamp batch sort intact.
     */
    private static final String CONTROL_KEY_PREFIX = "control-key-";

    /**
     * Serializer used to build and read back WAL records in this test; matches the {@code
     * ProtoStuffSerializer} that production {@code WALWriter}/{@code WALReader} instances use.
     */
    private static final Serializer SERIALIZER = new ProtoStuffSerializer();

    /**
     * Dedicated WAL directory used only by this test class so it cannot collide with other test
     * classes' fixtures, deleted again in {@link #close()} once the tests finish.
     */
    private static final Path SAME_TIMESTAMP_BATCH_PATH =
            new Path("/tmp/imap-wal-same-timestamp-collision-batch/");

    /**
     * Local Hadoop filesystem instance shared by both test methods below, initialized once in
     * {@link #init()} and closed again in {@link #close()} after the tests finish.
     */
    private static FileSystem FS;

    /**
     * Initializes the local Hadoop filesystem used to write and replay the WAL directory, mirroring
     * the setup used by the other real-storage tests in this module.
     *
     * @throws IOException if the local filesystem cannot be obtained
     */
    @BeforeAll
    public static void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FS = FileSystem.getLocal(conf);
    }

    /**
     * Directly sorts the 85-record same-timestamp batch and checks the sort both completes and
     * produces a result consistent with the {@code Comparable} contract: antisymmetry holds for
     * every put/delete pair, each pair's delete deterministically sorts ahead of its put, and the
     * fully sorted batch is non-decreasing end to end (a transitivity-implying self-consistency
     * check across all 85 records at once, not just one pair).
     *
     * @throws IOException if serializing a key or value while building the batch fails
     */
    @Test
    public void testCollectionsSortOfLargeSameTimestampBatchSatisfiesComparableContract()
            throws IOException {
        List<CollisionPair> pairs = buildCollisionPairs();
        List<IMapFileData> controls = buildControlRecords();

        List<IMapFileData> sorted = new ArrayList<>(pairs.size() * 2 + controls.size());
        for (CollisionPair pair : pairs) {
            sorted.add(pair.put);
            sorted.add(pair.delete);
        }
        sorted.addAll(controls);

        Assertions.assertDoesNotThrow(
                () -> Collections.sort(sorted),
                "sorting "
                        + sorted.size()
                        + " same-timestamp records must not throw once the contract is fixed");

        for (CollisionPair pair : pairs) {
            int putToDelete = Integer.signum(pair.put.compareTo(pair.delete));
            int deleteToPut = Integer.signum(pair.delete.compareTo(pair.put));
            Assertions.assertEquals(
                    -deleteToPut,
                    putToDelete,
                    "compareTo must be antisymmetric for key " + pair.key);
            Assertions.assertTrue(
                    pair.delete.compareTo(pair.put) < 0,
                    "a same-timestamp delete must deterministically sort before its put for key "
                            + pair.key);
            Assertions.assertTrue(
                    sorted.indexOf(pair.delete) < sorted.indexOf(pair.put),
                    "delete must precede put in the sorted batch for key " + pair.key);
        }

        // Self-consistency scan: a valid total order must be non-decreasing end to end once
        // sorted; this exercises transitivity across all 85 records simultaneously rather than
        // only spot-checking a handful of hand-picked triples.
        for (int i = 0; i < sorted.size() - 1; i++) {
            Assertions.assertTrue(
                    sorted.get(i).compareTo(sorted.get(i + 1)) <= 0,
                    "sorted batch must be non-decreasing at index " + i);
        }
    }

    /**
     * Writes the 40 same-timestamp put/delete pairs through the production {@code WALWriter},
     * alternating which record of each pair is appended first, then replays them through the
     * production {@code WALReader#loadAllData} and {@code #loadAllKeys} -- the exact methods
     * apache/seatunnel#11462 names as the crash site. Asserts the replay does not throw and that
     * every delete is genuinely observed (the key is absent from both results) no matter which
     * physical write order produced it, while the 5 control keys keep their values.
     *
     * @throws Exception if writing or reading the WAL fails, or if {@code WALWriter#close} fails
     */
    @Test
    public void testWalReplayDoesNotLoseSameTimestampDeletesAcrossWriteOrders() throws Exception {
        List<CollisionPair> pairs = buildCollisionPairs();
        List<IMapFileData> controls = buildControlRecords();

        try (WALWriter writer =
                new WALWriter(FS, FileConfiguration.HDFS, SAME_TIMESTAMP_BATCH_PATH, SERIALIZER)) {
            for (int i = 0; i < pairs.size(); i++) {
                CollisionPair pair = pairs.get(i);
                // Alternate physical WAL append order across pairs: even-indexed keys append the
                // put before the delete, odd-indexed keys append the delete before the put. The
                // fixed comparator is a pure function of record fields, not append position, so
                // both physical orders must resolve identically -- proving the outcome no longer
                // depends on the undefined behavior the broken comparator used to leave to
                // TimSort's internal implementation details.
                if (i % 2 == 0) {
                    writer.write(pair.put);
                    writer.write(pair.delete);
                } else {
                    writer.write(pair.delete);
                    writer.write(pair.put);
                }
            }
            for (IMapFileData control : controls) {
                writer.write(control);
            }
        }

        WALReader reader = new WALReader(FS, FileConfiguration.HDFS, SERIALIZER);

        AtomicReference<Map<Object, Object>> loadedData = new AtomicReference<>();
        Assertions.assertDoesNotThrow(
                () ->
                        loadedData.set(
                                reader.loadAllData(SAME_TIMESTAMP_BATCH_PATH, new HashSet<>())),
                "WALReader#loadAllData must not throw while sorting "
                        + (pairs.size() * 2 + controls.size())
                        + " same-timestamp records on replay");

        AtomicReference<Set<Object>> loadedKeys = new AtomicReference<>();
        Assertions.assertDoesNotThrow(
                () -> loadedKeys.set(reader.loadAllKeys(SAME_TIMESTAMP_BATCH_PATH)),
                "WALReader#loadAllKeys must not throw while sorting "
                        + (pairs.size() * 2 + controls.size())
                        + " same-timestamp records on replay");

        Map<Object, Object> data = loadedData.get();
        Set<Object> keys = loadedKeys.get();

        for (CollisionPair pair : pairs) {
            Assertions.assertFalse(
                    data.containsKey(pair.key),
                    "same-timestamp delete for "
                            + pair.key
                            + " must not be lost regardless of WAL write order");
            Assertions.assertFalse(
                    keys.contains(pair.key),
                    "same-timestamp delete for "
                            + pair.key
                            + " must not be lost regardless of WAL write order");
        }
        for (int j = 0; j < controls.size(); j++) {
            String key = CONTROL_KEY_PREFIX + j;
            Assertions.assertEquals("control-value-" + j, data.get(key));
            Assertions.assertTrue(keys.contains(key));
        }
        // Exact size assertions ensure no colliding record leaked into the result and no control
        // record was accidentally dropped by the batch sort/dedup.
        Assertions.assertEquals(controls.size(), data.size());
        Assertions.assertEquals(controls.size(), keys.size());
    }

    /**
     * Builds the 40 independent same-key put/delete pairs shared by both test methods. Every pair
     * uses its own unique key so pairs cannot interfere with each other, and every record in every
     * pair shares {@link #COLLISION_TIMESTAMP} so the comparator must fall through to its
     * deleted-before-not-deleted tie-breaker for each pair.
     *
     * @return the 40 collision pairs, in key order
     * @throws IOException if serializing a key or value fails
     */
    private List<CollisionPair> buildCollisionPairs() throws IOException {
        List<CollisionPair> pairs = new ArrayList<>(COLLISION_KEY_COUNT);
        for (int i = 0; i < COLLISION_KEY_COUNT; i++) {
            String key = COLLISION_KEY_PREFIX + i;
            IMapFileData put =
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(key))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize("value-" + i))
                            .valueClassName(String.class.getName())
                            .deleted(false)
                            .timestamp(COLLISION_TIMESTAMP)
                            .build();
            // Mirrors IMapFileStorage#buildDeleteIMapFileData: a tombstone carries no value.
            IMapFileData delete =
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(key))
                            .keyClassName(String.class.getName())
                            .deleted(true)
                            .timestamp(COLLISION_TIMESTAMP)
                            .build();
            pairs.add(new CollisionPair(key, put, delete));
        }
        return pairs;
    }

    /**
     * Builds the uncontested control records: one put per control key, no matching delete. These
     * ride along in the same same-timestamp batch so both tests also prove the large collision
     * batch does not corrupt or drop unrelated data.
     *
     * @return the control records, in key order
     * @throws IOException if serializing a key or value fails
     */
    private List<IMapFileData> buildControlRecords() throws IOException {
        List<IMapFileData> controls = new ArrayList<>(CONTROL_KEY_COUNT);
        for (int j = 0; j < CONTROL_KEY_COUNT; j++) {
            String key = CONTROL_KEY_PREFIX + j;
            controls.add(
                    IMapFileData.builder()
                            .key(SERIALIZER.serialize(key))
                            .keyClassName(String.class.getName())
                            .value(SERIALIZER.serialize("control-value-" + j))
                            .valueClassName(String.class.getName())
                            .deleted(false)
                            .timestamp(COLLISION_TIMESTAMP)
                            .build());
        }
        return controls;
    }

    /**
     * Deletes the dedicated WAL directory and closes the shared filesystem handle so this test
     * class leaves no state behind for later test classes in the same JVM.
     *
     * @throws IOException if deleting the WAL directory or closing the filesystem fails
     */
    @AfterAll
    public static void close() throws IOException {
        FS.delete(SAME_TIMESTAMP_BATCH_PATH, true);
        FS.close();
    }

    /**
     * A same-key put/delete pair sharing {@link #COLLISION_TIMESTAMP}. Bundles the deserialized
     * string key alongside both records so assertions can look up expected map/set membership
     * without re-deserializing the key bytes.
     */
    private static final class CollisionPair {

        /**
         * Deserialized string form of {@link #put} and {@link #delete}'s shared key, kept alongside
         * the raw records so assertions can look it up without re-deserializing the key bytes.
         */
        private final String key;

        /**
         * The put record of the pair, sharing {@link #key} and the batch timestamp with {@link
         * #delete}; it must never survive the WAL replay for this key.
         */
        private final IMapFileData put;

        /**
         * The same-timestamp tombstone for {@link #key} that must deterministically win over {@link
         * #put} during WAL replay, regardless of physical write order.
         */
        private final IMapFileData delete;

        private CollisionPair(String key, IMapFileData put, IMapFileData delete) {
            this.key = key;
            this.put = put;
            this.delete = delete;
        }
    }
}
