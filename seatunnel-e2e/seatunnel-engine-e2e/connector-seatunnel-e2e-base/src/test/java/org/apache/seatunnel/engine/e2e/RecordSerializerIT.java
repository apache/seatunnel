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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Slf4j
public class RecordSerializerIT {

    private static final String MAP_NAME = "test-record-serializer";
    private static HazelcastInstanceImpl instance1;
    private static HazelcastInstanceImpl instance2;

    @BeforeAll
    static void setUp() {
        String clusterName = TestUtils.getClusterName("RecordSerializerIT_hzSerializationTest");
        instance1 = createHazelcastInstance(clusterName);
        instance2 = createHazelcastInstance(clusterName);
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> instance1.getCluster().getMembers().size() == 2);
    }

    @AfterAll
    static void tearDown() {
        if (instance1 != null) {
            instance1.shutdown();
        }
        if (instance2 != null) {
            instance2.shutdown();
        }
    }

    @Test
    public void testSeaTunnelRowRoundTrip() {
        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setTableId("test_db.test_table");
        row.setRowKind(RowKind.INSERT);
        row.setField(0, "hello");
        row.setField(1, 42);
        row.setField(2, 3.14);

        Record<SeaTunnelRow> original = new Record<>(row);

        IMap<String, Record<?>> writerMap = instance1.getMap(MAP_NAME);
        IMap<String, Record<?>> readerMap = instance2.getMap(MAP_NAME);

        String key = "row-insert";
        writerMap.put(key, original);

        await().atMost(10, TimeUnit.SECONDS).until(() -> readerMap.containsKey(key));

        Record<?> deserialized = readerMap.get(key);
        Assertions.assertNotNull(deserialized);
        Assertions.assertInstanceOf(SeaTunnelRow.class, deserialized.getData());

        SeaTunnelRow actual = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals("test_db.test_table", actual.getTableId());
        Assertions.assertEquals(RowKind.INSERT, actual.getRowKind());
        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertEquals("hello", actual.getField(0));
        Assertions.assertEquals(42, actual.getField(1));
        Assertions.assertEquals(3.14, actual.getField(2));
        Assertions.assertEquals(row, actual);

        writerMap.remove(key);
    }

    @Test
    public void testSeaTunnelRowAllRowKinds() {
        for (RowKind rowKind : RowKind.values()) {
            SeaTunnelRow row = new SeaTunnelRow(1);
            row.setTableId("db.tbl");
            row.setRowKind(rowKind);
            row.setField(0, rowKind.shortString());

            Record<SeaTunnelRow> original = new Record<>(row);

            IMap<String, Record<?>> writerMap = instance1.getMap(MAP_NAME);
            IMap<String, Record<?>> readerMap = instance2.getMap(MAP_NAME);

            String key = "row-kind-" + rowKind.name();
            writerMap.put(key, original);

            await().atMost(10, TimeUnit.SECONDS).until(() -> readerMap.containsKey(key));

            Record<?> deserialized = readerMap.get(key);
            SeaTunnelRow actual = (SeaTunnelRow) deserialized.getData();
            Assertions.assertEquals("db.tbl", actual.getTableId());
            Assertions.assertEquals(rowKind, actual.getRowKind());
            Assertions.assertEquals(1, actual.getArity());
            Assertions.assertEquals(rowKind.shortString(), actual.getField(0));
            Assertions.assertEquals(row, actual);

            writerMap.remove(key);
        }
    }

    @Test
    public void testCheckpointBarrierRoundTrip() {
        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        100L,
                        System.currentTimeMillis(),
                        CheckpointType.CHECKPOINT_TYPE,
                        Collections.emptySet(),
                        Collections.emptySet());

        Record<CheckpointBarrier> original = new Record<>(barrier);

        IMap<String, Record<?>> writerMap = instance1.getMap(MAP_NAME);
        IMap<String, Record<?>> readerMap = instance2.getMap(MAP_NAME);

        String key = "barrier-checkpoint";
        writerMap.put(key, original);

        await().atMost(10, TimeUnit.SECONDS).until(() -> readerMap.containsKey(key));

        Record<?> deserialized = readerMap.get(key);
        Assertions.assertNotNull(deserialized);
        Assertions.assertInstanceOf(CheckpointBarrier.class, deserialized.getData());

        CheckpointBarrier actual = (CheckpointBarrier) deserialized.getData();
        Assertions.assertEquals(barrier.getId(), actual.getId());
        Assertions.assertEquals(barrier.getTimestamp(), actual.getTimestamp());
        Assertions.assertEquals(CheckpointType.CHECKPOINT_TYPE, actual.getCheckpointType());
        Assertions.assertEquals(Collections.emptySet(), actual.getPrepareCloseTasks());
        Assertions.assertEquals(Collections.emptySet(), actual.getClosedTasks());
        Assertions.assertEquals(barrier, actual);

        writerMap.remove(key);
    }

    @Test
    public void testCheckpointBarrierWithTaskLocations() {
        TaskGroupLocation groupLoc1 = new TaskGroupLocation(1L, 1, 100L);
        TaskGroupLocation groupLoc2 = new TaskGroupLocation(1L, 1, 200L);
        TaskLocation taskLoc1 = new TaskLocation(groupLoc1, 1L, 0);
        TaskLocation taskLoc2 = new TaskLocation(groupLoc2, 2L, 1);

        Set<TaskLocation> prepareClose = new HashSet<>();
        prepareClose.add(taskLoc1);
        Set<TaskLocation> closed = new HashSet<>();
        closed.add(taskLoc2);

        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        300L,
                        System.currentTimeMillis(),
                        CheckpointType.SAVEPOINT_TYPE,
                        prepareClose,
                        closed);

        Record<CheckpointBarrier> original = new Record<>(barrier);

        IMap<String, Record<?>> writerMap = instance1.getMap(MAP_NAME);
        IMap<String, Record<?>> readerMap = instance2.getMap(MAP_NAME);

        String key = "barrier-with-tasks";
        writerMap.put(key, original);

        await().atMost(10, TimeUnit.SECONDS).until(() -> readerMap.containsKey(key));

        Record<?> deserialized = readerMap.get(key);
        Assertions.assertNotNull(deserialized);
        Assertions.assertInstanceOf(CheckpointBarrier.class, deserialized.getData());

        CheckpointBarrier actual = (CheckpointBarrier) deserialized.getData();
        Assertions.assertEquals(300L, actual.getId());
        Assertions.assertEquals(barrier.getTimestamp(), actual.getTimestamp());
        Assertions.assertEquals(CheckpointType.SAVEPOINT_TYPE, actual.getCheckpointType());
        Assertions.assertEquals(1, actual.getPrepareCloseTasks().size());
        Assertions.assertTrue(actual.getPrepareCloseTasks().contains(taskLoc1));
        Assertions.assertEquals(1, actual.getClosedTasks().size());
        Assertions.assertTrue(actual.getClosedTasks().contains(taskLoc2));

        writerMap.remove(key);
    }

    @Test
    public void testCheckpointBarrierAllTypes() {
        for (CheckpointType type : CheckpointType.values()) {
            CheckpointBarrier barrier =
                    new CheckpointBarrier(
                            200L + type.ordinal(),
                            System.currentTimeMillis(),
                            type,
                            Collections.emptySet(),
                            Collections.emptySet());

            Record<CheckpointBarrier> original = new Record<>(barrier);

            IMap<String, Record<?>> writerMap = instance1.getMap(MAP_NAME);
            IMap<String, Record<?>> readerMap = instance2.getMap(MAP_NAME);

            String key = "barrier-type-" + type.name();
            writerMap.put(key, original);

            await().atMost(10, TimeUnit.SECONDS).until(() -> readerMap.containsKey(key));

            Record<?> deserialized = readerMap.get(key);
            CheckpointBarrier actual = (CheckpointBarrier) deserialized.getData();
            Assertions.assertEquals(type, actual.getCheckpointType());
            Assertions.assertEquals(200L + type.ordinal(), actual.getId());
            Assertions.assertEquals(barrier.getTimestamp(), actual.getTimestamp());
            Assertions.assertEquals(Collections.emptySet(), actual.getPrepareCloseTasks());
            Assertions.assertEquals(Collections.emptySet(), actual.getClosedTasks());
            Assertions.assertEquals(barrier, actual);

            writerMap.remove(key);
        }
    }

    @Test
    public void testFlushSignalRoundTrip() {
        FlushSignal flushSignal = new FlushSignal(12345L, 67890L, System.currentTimeMillis());

        Record<FlushSignal> original = new Record<>(flushSignal);

        IMap<String, Record<?>> writerMap = instance1.getMap(MAP_NAME);
        IMap<String, Record<?>> readerMap = instance2.getMap(MAP_NAME);

        String key = "flush-signal";
        writerMap.put(key, original);

        await().atMost(10, TimeUnit.SECONDS).until(() -> readerMap.containsKey(key));

        Record<?> deserialized = readerMap.get(key);
        Assertions.assertNotNull(deserialized);
        Assertions.assertInstanceOf(FlushSignal.class, deserialized.getData());

        FlushSignal actual = (FlushSignal) deserialized.getData();
        Assertions.assertEquals(12345L, actual.getJobId());
        Assertions.assertEquals(67890L, actual.getTaskId());
        Assertions.assertEquals(flushSignal.getCreatedTime(), actual.getCreatedTime());
        Assertions.assertEquals(flushSignal, actual);

        writerMap.remove(key);
    }

    @Test
    public void testSeaTunnelRowWithNullFields() {
        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setTableId("db.nullable_table");
        row.setRowKind(RowKind.INSERT);
        row.setField(0, null);
        row.setField(1, "non-null");
        row.setField(2, null);

        Record<SeaTunnelRow> original = new Record<>(row);

        IMap<String, Record<?>> writerMap = instance1.getMap(MAP_NAME);
        IMap<String, Record<?>> readerMap = instance2.getMap(MAP_NAME);

        String key = "row-nullable";
        writerMap.put(key, original);

        await().atMost(10, TimeUnit.SECONDS).until(() -> readerMap.containsKey(key));

        Record<?> deserialized = readerMap.get(key);
        Assertions.assertNotNull(deserialized);
        Assertions.assertInstanceOf(SeaTunnelRow.class, deserialized.getData());

        SeaTunnelRow actual = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals("db.nullable_table", actual.getTableId());
        Assertions.assertEquals(RowKind.INSERT, actual.getRowKind());
        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertNull(actual.getField(0));
        Assertions.assertEquals("non-null", actual.getField(1));
        Assertions.assertNull(actual.getField(2));
        Assertions.assertEquals(row, actual);

        writerMap.remove(key);
    }

    private static HazelcastInstanceImpl createHazelcastInstance(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        Config hazelcastConfig = Config.loadFromString(buildHazelcastConfig(clusterName));
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        return SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    private static String buildHazelcastConfig(String clusterName) {
        return "hazelcast:\n"
                + "  cluster-name: "
                + clusterName
                + "\n"
                + "  network:\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - localhost\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: 5901\n";
    }
}
