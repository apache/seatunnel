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

package org.apache.seatunnel.engine.e2e.timerflush;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

public class TimerFlushIT {

    private static final String CLUSTER_NAME = "TimerFlushIT";

    private HazelcastInstanceImpl hazelcastInstance;
    private SeaTunnelConfig seaTunnelConfig;

    @BeforeEach
    void setUp() {
        TimerFlushTestSinkWriter.reset();
        MultiTableFlushTestSinkWriter.reset();
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void testFlushTriggeredByEngineTimer() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("timer_flush_enabled.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("timer-flush-enabled");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));

        try (SeaTunnelClient client = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment env =
                    client.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = env.execute();

            // Timer flush can run before any rows reach the sink (empty buffer flush still
            // increments FLUSH_COUNT). Wait until a flush actually moved rows into FLUSHED_ROWS.
            await().atMost(15, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            TimerFlushTestSinkWriter.FLUSHED_ROWS.isEmpty(),
                                            "Expected at least one flush with buffered rows; flushCount="
                                                    + TimerFlushTestSinkWriter.FLUSH_COUNT.get()));

            jobProxy.cancelJob();
        }
    }

    @Test
    void testNoFlushWhenTimerDisabled() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("timer_flush_disabled.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("timer-flush-disabled");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));

        try (SeaTunnelClient client = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment env =
                    client.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = env.execute();

            // Assert that flushCount stays at 0 for 8 continuous seconds.
            // No timer is registered when sink.flush.interval is unset, so nothing should leak.
            Awaitility.await()
                    .during(8, TimeUnit.SECONDS)
                    .atMost(9, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .until(() -> TimerFlushTestSinkWriter.FLUSH_COUNT.get() == 0);

            Assertions.assertTrue(
                    TimerFlushTestSinkWriter.FLUSHED_ROWS.isEmpty(),
                    "Flushed rows must remain empty when timer flush is disabled");

            jobProxy.cancelJob();
        }
    }

    @Test
    void testMultiTableAggregatedFlush() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("timer_flush_multi_table.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("timer-flush-multi-table");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));

        try (SeaTunnelClient client = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment env =
                    client.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = env.execute();

            // 1. Wait until all three tables have been flushed at least 3 times
            String[] tableIds = {"db1.table_a", "db1.table_b", "db1.table_c"};
            await().atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                for (String tableId : tableIds) {
                                    AtomicInteger count =
                                            MultiTableFlushTestSinkWriter.FLUSH_COUNTS.get(tableId);
                                    Assertions.assertNotNull(
                                            count, tableId + " should have been flushed");
                                    Assertions.assertTrue(
                                            count.get() >= 3,
                                            tableId
                                                    + " flush count should be >= 3, got: "
                                                    + count.get());
                                }
                            });

            // 2. Every flush snapshot must contain at least one row (queue drain before flush)
            List<MultiTableFlushTestSinkWriter.FlushSnapshot> snapshots =
                    MultiTableFlushTestSinkWriter.FLUSH_SNAPSHOTS;
            Assertions.assertFalse(snapshots.isEmpty(), "Should have recorded flush snapshots");
            for (int i = 0; i < snapshots.size(); i++) {
                MultiTableFlushTestSinkWriter.FlushSnapshot snap = snapshots.get(i);
                Assertions.assertFalse(
                        snap.tableCounts.isEmpty(),
                        "Flush snapshot #" + i + " should have table counts");
                for (MultiTableFlushTestSinkWriter.FlushSnapshot.TableCount tc : snap.tableCounts) {
                    Assertions.assertTrue(
                            tc.rowCount > 0,
                            "Flush snapshot #"
                                    + i
                                    + " table="
                                    + tc.tableId
                                    + " should have rowCount > 0, got: "
                                    + tc.rowCount);
                }
            }

            // 3. Verify concurrency: multiple writer instances participated in flushing
            Set<String> flushThreads =
                    snapshots.stream().map(s -> s.threadName).collect(Collectors.toSet());
            Assertions.assertTrue(
                    flushThreads.size() >= 1,
                    "At least one flush thread expected, got: " + flushThreads);

            // 4. No row loss: flushed row totals <= written row totals for each table
            for (String tableId : MultiTableFlushTestSinkWriter.WRITE_COUNTS.keySet()) {
                AtomicLong written = MultiTableFlushTestSinkWriter.WRITE_COUNTS.get(tableId);
                AtomicLong flushed = MultiTableFlushTestSinkWriter.FLUSHED_ROW_TOTALS.get(tableId);
                Assertions.assertNotNull(
                        flushed, "Table " + tableId + " should have flushed row totals");
                Assertions.assertTrue(
                        flushed.get() <= written.get(),
                        "Table "
                                + tableId
                                + " flushed rows ("
                                + flushed.get()
                                + ") should not exceed written rows ("
                                + written.get()
                                + ")");
                Assertions.assertTrue(
                        flushed.get() > 0,
                        "Table " + tableId + " should have flushed at least some rows");
            }

            jobProxy.cancelJob();
        }
    }
}
