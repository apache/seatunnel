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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Covers CDC-specific Zeta streaming failover paths by combining the existing local Hazelcast
 * fault-injection harness with a real MySQL CDC source and JDBC sink.
 */
@Slf4j
public class MysqlCDCClusterFailoverIT {

    /** Streaming single-pipeline CDC template used by the failover tests. */
    private static final String SINGLE_PIPELINE_TEMPLATE =
            "mysql-cdc-failover/stream_mysqlcdc_to_mysql_failover_template.conf";

    /** Streaming two-pipeline CDC template used by the multi-pipeline failover tests. */
    private static final String TWO_PIPELINE_TEMPLATE =
            "mysql-cdc-failover/stream_mysqlcdc_to_mysql_failover_two_pipeline_template.conf";

    /** Root username used by the test driver for schema and data preparation. */
    private static final String ROOT_USERNAME = "mysqluser";

    /** Root password used by the test driver for schema and data preparation. */
    private static final String ROOT_PASSWORD = "mysqlpw";

    /** Shared CDC-enabled MySQL test container for the engine failover suite. */
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    /**
     * Starts the shared CDC-enabled MySQL container once for the suite so every failover scenario
     * sees identical source capabilities.
     */
    @BeforeAll
    public static void startUp() {
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
    }

    /** Stops the shared MySQL container after the suite completes. */
    @AfterAll
    public static void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    /**
     * Verifies that a single-pipeline CDC job keeps running and advancing checkpoints after a
     * worker failover in the default two-node cluster.
     */
    @Test
    public void testMysqlCdcStreamJobRestoreIn2NodeWorkerDown() throws Exception {
        runSinglePipelineRegularFailoverTest(
                "testMysqlCdcStreamJobRestoreIn2NodeWorkerDown", FailoverTarget.WORKER);
    }

    /**
     * Verifies that a single-pipeline CDC job keeps running and advancing checkpoints after a
     * master failover in the default two-node cluster.
     */
    @Test
    public void testMysqlCdcStreamJobRestoreIn2NodeMasterDown() throws Exception {
        runSinglePipelineRegularFailoverTest(
                "testMysqlCdcStreamJobRestoreIn2NodeMasterDown", FailoverTarget.MASTER);
    }

    /**
     * Verifies that a single-pipeline CDC job can recover after every node in the default cluster
     * restarts from persistent state.
     */
    @Test
    public void testMysqlCdcStreamJobRestoreInAllNodeDown() throws Exception {
        runSinglePipelineRegularAllNodeDownTest("testMysqlCdcStreamJobRestoreInAllNodeDown");
    }

    /**
     * Verifies that a split master/worker cluster keeps a single-pipeline CDC job running after a
     * worker failover.
     */
    @Test
    public void testMysqlCdcSplitClusterStreamJobRestoreInWorkerDown() throws Exception {
        runSinglePipelineSplitClusterFailoverTest(
                "testMysqlCdcSplitClusterStreamJobRestoreInWorkerDown", FailoverTarget.WORKER);
    }

    /**
     * Verifies that a split master/worker cluster keeps a single-pipeline CDC job running after a
     * master failover.
     */
    @Test
    public void testMysqlCdcSplitClusterStreamJobRestoreInMasterDown() throws Exception {
        runSinglePipelineSplitClusterFailoverTest(
                "testMysqlCdcSplitClusterStreamJobRestoreInMasterDown", FailoverTarget.MASTER);
    }

    /**
     * Verifies that a split master/worker cluster can recover a single-pipeline CDC job after all
     * members restart from persistent state.
     */
    @Test
    public void testMysqlCdcSplitClusterStreamJobRestoreInAllNodeDown() throws Exception {
        runSinglePipelineSplitClusterAllNodeDownTest(
                "testMysqlCdcSplitClusterStreamJobRestoreInAllNodeDown");
    }

    /**
     * Verifies that a two-pipeline CDC job keeps both pipelines consistent after a worker failover
     * in the default two-node cluster.
     */
    @Test
    public void testMysqlCdcTwoPipelineStreamJobRestoreIn2NodeWorkerDown() throws Exception {
        runTwoPipelineRegularFailoverTest(
                "testMysqlCdcTwoPipelineStreamJobRestoreIn2NodeWorkerDown", FailoverTarget.WORKER);
    }

    /**
     * Verifies that a two-pipeline CDC job keeps both pipelines consistent after a master failover
     * in the default two-node cluster.
     */
    @Test
    public void testMysqlCdcTwoPipelineStreamJobRestoreIn2NodeMasterDown() throws Exception {
        runTwoPipelineRegularFailoverTest(
                "testMysqlCdcTwoPipelineStreamJobRestoreIn2NodeMasterDown", FailoverTarget.MASTER);
    }

    /**
     * Verifies that a two-pipeline CDC job can recover after every node in the default cluster
     * restarts from persistent state.
     */
    @Test
    public void testMysqlCdcTwoPipelineStreamJobRestoreInAllNodeDown() throws Exception {
        runTwoPipelineRegularAllNodeDownTest(
                "testMysqlCdcTwoPipelineStreamJobRestoreInAllNodeDown");
    }

    /**
     * Verifies that a split master/worker cluster keeps both CDC pipelines consistent after a
     * worker failover.
     */
    @Test
    public void testMysqlCdcSplitClusterTwoPipelineStreamJobRestoreInWorkerDown() throws Exception {
        runTwoPipelineSplitClusterFailoverTest(
                "testMysqlCdcSplitClusterTwoPipelineStreamJobRestoreInWorkerDown",
                FailoverTarget.WORKER);
    }

    /**
     * Verifies that a split master/worker cluster keeps both CDC pipelines consistent after a
     * master failover.
     */
    @Test
    public void testMysqlCdcSplitClusterTwoPipelineStreamJobRestoreInMasterDown() throws Exception {
        runTwoPipelineSplitClusterFailoverTest(
                "testMysqlCdcSplitClusterTwoPipelineStreamJobRestoreInMasterDown",
                FailoverTarget.MASTER);
    }

    /**
     * Verifies that a split master/worker cluster can recover both CDC pipelines after every member
     * restarts from persistent state.
     */
    @Test
    public void testMysqlCdcSplitClusterTwoPipelineStreamJobRestoreInAllNodeDown()
            throws Exception {
        runTwoPipelineSplitClusterAllNodeDownTest(
                "testMysqlCdcSplitClusterTwoPipelineStreamJobRestoreInAllNodeDown");
    }

    /**
     * Runs the regular two-node streaming CDC failover test for a single pipeline.
     *
     * @param testCaseName stable test case name used for config files and database names
     * @param failoverTarget whether to fail the worker-side node or the initial master node
     */
    private void runSinglePipelineRegularFailoverTest(
            String testCaseName, FailoverTarget failoverTarget) throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TableBinding tableBinding = createSinglePipelineBinding(testCaseName);
        insertRows(tableBinding, 1, 2, 3);

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig config1 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig config2 = createSeaTunnelConfig(clusterName);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(config1);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(config2);
            awaitClusterSize(node1, 2);

            String configPath = createSinglePipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, config1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding, 11, 12);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(node1), jobId, 1);

            HazelcastInstanceImpl survivingNode;
            if (failoverTarget == FailoverTarget.MASTER) {
                node1.shutdown();
                node1 = null;
                survivingNode = node2;
            } else {
                node2.shutdown();
                node2 = null;
                survivingNode = node1;
            }

            awaitClusterSize(survivingNode, 1);
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(checkpointCounterStore(survivingNode), jobId, checkpointBefore);

            insertRows(tableBinding, 21, 22);
            awaitSourceAndSinkConsistent(tableBinding);

            clientJobProxy.cancelJob();
            awaitJobStatus(clientJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(node1);
            shutdownNode(node2);
        }
    }

    /**
     * Runs the split-cluster streaming CDC failover test for a single pipeline.
     *
     * @param testCaseName stable test case name used for config files and database names
     * @param failoverTarget whether to fail the worker-side node or the initial master node
     */
    private void runSinglePipelineSplitClusterFailoverTest(
            String testCaseName, FailoverTarget failoverTarget) throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TableBinding tableBinding = createSinglePipelineBinding(testCaseName);
        insertRows(tableBinding, 1, 2, 3);

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig masterConfig1 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig masterConfig2 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig1 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig2 = createSeaTunnelConfig(clusterName);

            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig1);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig2);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig1);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig2);
            awaitClusterSize(masterNode1, 4);

            String configPath = createSinglePipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, masterConfig1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding, 11, 12);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(masterNode1), jobId, 1);

            HazelcastInstanceImpl survivingMaster;
            if (failoverTarget == FailoverTarget.MASTER) {
                masterNode1.shutdown();
                masterNode1 = null;
                survivingMaster = masterNode2;
            } else {
                workerNode1.shutdown();
                workerNode1 = null;
                survivingMaster = masterNode1;
            }

            awaitClusterSize(survivingMaster, 3);
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(
                    checkpointCounterStore(survivingMaster), jobId, checkpointBefore);

            insertRows(tableBinding, 21, 22);
            awaitSourceAndSinkConsistent(tableBinding);

            clientJobProxy.cancelJob();
            awaitJobStatus(clientJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(masterNode1);
            shutdownNode(masterNode2);
            shutdownNode(workerNode1);
            shutdownNode(workerNode2);
        }
    }

    /**
     * Runs the regular two-node all-node-down recovery test for a single CDC pipeline.
     *
     * @param testCaseName stable test case name used for config files and database names
     */
    private void runSinglePipelineRegularAllNodeDownTest(String testCaseName) throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TableBinding tableBinding = createSinglePipelineBinding(testCaseName);
        insertRows(tableBinding, 1, 2, 3);

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig config1 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig config2 = createPersistentSeaTunnelConfig(clusterName);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(config1);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(config2);
            awaitClusterSize(node1, 2);

            String configPath = createSinglePipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, config1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding, 11, 12);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(node1), jobId, 1);

            node1.shutdown();
            node2.shutdown();
            node1 = null;
            node2 = null;
            closeClient(engineClient);
            engineClient = null;

            insertRows(tableBinding, 21, 22);

            config1 = createPersistentSeaTunnelConfig(clusterName);
            config2 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig restartedConfig1 = config1;
            SeaTunnelConfig restartedConfig2 = config2;
            node1 =
                    startNodeWithRetry(
                            "regular-master-1",
                            () -> SeaTunnelServerStarter.createHazelcastInstance(restartedConfig1));
            node2 =
                    startNodeWithRetry(
                            "regular-master-2",
                            () -> SeaTunnelServerStarter.createHazelcastInstance(restartedConfig2));
            awaitClusterSize(node1, 2);

            engineClient = createEngineClient(clusterName);
            ClientJobProxy restoredJobProxy = engineClient.createJobClient().getJobProxy(jobId);
            awaitJobStatus(restoredJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(checkpointCounterStore(node1), jobId, checkpointBefore);

            insertRows(tableBinding, 31, 32);
            awaitSourceAndSinkConsistent(tableBinding);

            restoredJobProxy.cancelJob();
            awaitJobStatus(restoredJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(node1);
            shutdownNode(node2);
        }
    }

    /**
     * Runs the split-cluster all-node-down recovery test for a single CDC pipeline.
     *
     * @param testCaseName stable test case name used for config files and database names
     */
    private void runSinglePipelineSplitClusterAllNodeDownTest(String testCaseName)
            throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TableBinding tableBinding = createSinglePipelineBinding(testCaseName);
        insertRows(tableBinding, 1, 2, 3);

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig masterConfig1 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig masterConfig2 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig1 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig2 = createPersistentSeaTunnelConfig(clusterName);

            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig1);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig2);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig1);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig2);
            awaitClusterSize(masterNode1, 4);

            String configPath = createSinglePipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, masterConfig1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding, 11, 12);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(masterNode1), jobId, 1);

            masterNode1.shutdown();
            masterNode2.shutdown();
            workerNode1.shutdown();
            workerNode2.shutdown();
            masterNode1 = null;
            masterNode2 = null;
            workerNode1 = null;
            workerNode2 = null;
            closeClient(engineClient);
            engineClient = null;

            insertRows(tableBinding, 21, 22);

            masterConfig1 = createPersistentSeaTunnelConfig(clusterName);
            masterConfig2 = createPersistentSeaTunnelConfig(clusterName);
            workerConfig1 = createPersistentSeaTunnelConfig(clusterName);
            workerConfig2 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig restartedMasterConfig1 = masterConfig1;
            SeaTunnelConfig restartedMasterConfig2 = masterConfig2;
            SeaTunnelConfig restartedWorkerConfig1 = workerConfig1;
            SeaTunnelConfig restartedWorkerConfig2 = workerConfig2;

            masterNode1 =
                    startNodeWithRetry(
                            "split-master-1",
                            () ->
                                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                                            restartedMasterConfig1));
            masterNode2 =
                    startNodeWithRetry(
                            "split-master-2",
                            () ->
                                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                                            restartedMasterConfig2));
            workerNode1 =
                    startNodeWithRetry(
                            "split-worker-1",
                            () ->
                                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                                            restartedWorkerConfig1));
            workerNode2 =
                    startNodeWithRetry(
                            "split-worker-2",
                            () ->
                                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                                            restartedWorkerConfig2));
            awaitClusterSize(masterNode1, 4);

            engineClient = createEngineClient(clusterName);
            ClientJobProxy restoredJobProxy = engineClient.createJobClient().getJobProxy(jobId);
            awaitJobStatus(restoredJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(checkpointCounterStore(masterNode1), jobId, checkpointBefore);

            insertRows(tableBinding, 31, 32);
            awaitSourceAndSinkConsistent(tableBinding);

            restoredJobProxy.cancelJob();
            awaitJobStatus(restoredJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(masterNode1);
            shutdownNode(masterNode2);
            shutdownNode(workerNode1);
            shutdownNode(workerNode2);
        }
    }

    /**
     * Runs the regular two-node streaming CDC failover test for a two-pipeline job.
     *
     * @param testCaseName stable test case name used for config files and database names
     * @param failoverTarget whether to fail the worker-side node or the initial master node
     */
    private void runTwoPipelineRegularFailoverTest(
            String testCaseName, FailoverTarget failoverTarget) throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TwoTableBinding tableBinding = createTwoPipelineBinding(testCaseName);
        insertRows(tableBinding.first, 1, 2, 3);
        insertRows(tableBinding.second, 101, 102, 103);

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig config1 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig config2 = createSeaTunnelConfig(clusterName);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(config1);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(config2);
            awaitClusterSize(node1, 2);

            String configPath = createTwoPipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, config1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding.first, 11, 12);
            insertRows(tableBinding.second, 111, 112);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(node1), jobId, 2);

            HazelcastInstanceImpl survivingNode;
            if (failoverTarget == FailoverTarget.MASTER) {
                node1.shutdown();
                node1 = null;
                survivingNode = node2;
            } else {
                node2.shutdown();
                node2 = null;
                survivingNode = node1;
            }

            awaitClusterSize(survivingNode, 1);
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(checkpointCounterStore(survivingNode), jobId, checkpointBefore);

            insertRows(tableBinding.first, 21, 22);
            insertRows(tableBinding.second, 121, 122);
            awaitSourceAndSinkConsistent(tableBinding);

            clientJobProxy.cancelJob();
            awaitJobStatus(clientJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(node1);
            shutdownNode(node2);
        }
    }

    /**
     * Runs the regular two-node all-node-down recovery test for a two-pipeline CDC job.
     *
     * @param testCaseName stable test case name used for config files and database names
     */
    private void runTwoPipelineRegularAllNodeDownTest(String testCaseName) throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TwoTableBinding tableBinding = createTwoPipelineBinding(testCaseName);
        insertRows(tableBinding.first, 1, 2, 3);
        insertRows(tableBinding.second, 101, 102, 103);

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig config1 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig config2 = createPersistentSeaTunnelConfig(clusterName);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(config1);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(config2);
            awaitClusterSize(node1, 2);

            String configPath = createTwoPipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, config1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding.first, 11, 12);
            insertRows(tableBinding.second, 111, 112);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(node1), jobId, 2);

            node1.shutdown();
            node2.shutdown();
            node1 = null;
            node2 = null;
            closeClient(engineClient);
            engineClient = null;

            insertRows(tableBinding.first, 21, 22);
            insertRows(tableBinding.second, 121, 122);

            config1 = createPersistentSeaTunnelConfig(clusterName);
            config2 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig restartedConfig1 = config1;
            SeaTunnelConfig restartedConfig2 = config2;
            node1 =
                    startNodeWithRetry(
                            "regular-two-pipeline-master-1",
                            () -> SeaTunnelServerStarter.createHazelcastInstance(restartedConfig1));
            node2 =
                    startNodeWithRetry(
                            "regular-two-pipeline-master-2",
                            () -> SeaTunnelServerStarter.createHazelcastInstance(restartedConfig2));
            awaitClusterSize(node1, 2);

            engineClient = createEngineClient(clusterName);
            ClientJobProxy restoredJobProxy = engineClient.createJobClient().getJobProxy(jobId);
            awaitJobStatus(restoredJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(checkpointCounterStore(node1), jobId, checkpointBefore);

            insertRows(tableBinding.first, 31, 32);
            insertRows(tableBinding.second, 131, 132);
            awaitSourceAndSinkConsistent(tableBinding);

            restoredJobProxy.cancelJob();
            awaitJobStatus(restoredJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(node1);
            shutdownNode(node2);
        }
    }

    /**
     * Runs the split-cluster streaming CDC failover test for a two-pipeline job.
     *
     * @param testCaseName stable test case name used for config files and database names
     * @param failoverTarget whether to fail the worker-side node or the initial master node
     */
    private void runTwoPipelineSplitClusterFailoverTest(
            String testCaseName, FailoverTarget failoverTarget) throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TwoTableBinding tableBinding = createTwoPipelineBinding(testCaseName);
        insertRows(tableBinding.first, 1, 2, 3);
        insertRows(tableBinding.second, 101, 102, 103);

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig masterConfig1 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig masterConfig2 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig1 = createSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig2 = createSeaTunnelConfig(clusterName);

            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig1);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig2);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig1);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig2);
            awaitClusterSize(masterNode1, 4);

            String configPath = createTwoPipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, masterConfig1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding.first, 11, 12);
            insertRows(tableBinding.second, 111, 112);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(masterNode1), jobId, 2);

            HazelcastInstanceImpl survivingMaster;
            if (failoverTarget == FailoverTarget.MASTER) {
                masterNode1.shutdown();
                masterNode1 = null;
                survivingMaster = masterNode2;
            } else {
                workerNode1.shutdown();
                workerNode1 = null;
                survivingMaster = masterNode1;
            }

            awaitClusterSize(survivingMaster, 3);
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(
                    checkpointCounterStore(survivingMaster), jobId, checkpointBefore);

            insertRows(tableBinding.first, 21, 22);
            insertRows(tableBinding.second, 121, 122);
            awaitSourceAndSinkConsistent(tableBinding);

            clientJobProxy.cancelJob();
            awaitJobStatus(clientJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(masterNode1);
            shutdownNode(masterNode2);
            shutdownNode(workerNode1);
            shutdownNode(workerNode2);
        }
    }

    /**
     * Runs the split-cluster all-node-down recovery test for a two-pipeline CDC job.
     *
     * @param testCaseName stable test case name used for config files and database names
     */
    private void runTwoPipelineSplitClusterAllNodeDownTest(String testCaseName) throws Exception {
        String clusterName =
                TestUtils.getClusterName(testCaseName + "_" + System.currentTimeMillis());
        TwoTableBinding tableBinding = createTwoPipelineBinding(testCaseName);
        insertRows(tableBinding.first, 1, 2, 3);
        insertRows(tableBinding.second, 101, 102, 103);

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig masterConfig1 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig masterConfig2 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig1 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerConfig2 = createPersistentSeaTunnelConfig(clusterName);

            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig1);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig2);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig1);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig2);
            awaitClusterSize(masterNode1, 4);

            String configPath = createTwoPipelineConfig(testCaseName, tableBinding);
            engineClient = createEngineClient(clusterName);
            ClientJobProxy clientJobProxy =
                    submitJob(engineClient, masterConfig1, configPath, testCaseName);
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitSourceAndSinkConsistent(tableBinding);
            insertRows(tableBinding.first, 11, 12);
            insertRows(tableBinding.second, 111, 112);
            awaitSourceAndSinkConsistent(tableBinding);
            Map<Integer, Long> checkpointBefore =
                    awaitCheckpointIds(checkpointCounterStore(masterNode1), jobId, 2);

            masterNode1.shutdown();
            masterNode2.shutdown();
            workerNode1.shutdown();
            workerNode2.shutdown();
            masterNode1 = null;
            masterNode2 = null;
            workerNode1 = null;
            workerNode2 = null;
            closeClient(engineClient);
            engineClient = null;

            insertRows(tableBinding.first, 21, 22);
            insertRows(tableBinding.second, 121, 122);

            masterConfig1 = createPersistentSeaTunnelConfig(clusterName);
            masterConfig2 = createPersistentSeaTunnelConfig(clusterName);
            workerConfig1 = createPersistentSeaTunnelConfig(clusterName);
            workerConfig2 = createPersistentSeaTunnelConfig(clusterName);
            SeaTunnelConfig restartedMasterConfig1 = masterConfig1;
            SeaTunnelConfig restartedMasterConfig2 = masterConfig2;
            SeaTunnelConfig restartedWorkerConfig1 = workerConfig1;
            SeaTunnelConfig restartedWorkerConfig2 = workerConfig2;

            masterNode1 =
                    startNodeWithRetry(
                            "split-two-pipeline-master-1",
                            () ->
                                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                                            restartedMasterConfig1));
            masterNode2 =
                    startNodeWithRetry(
                            "split-two-pipeline-master-2",
                            () ->
                                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                                            restartedMasterConfig2));
            workerNode1 =
                    startNodeWithRetry(
                            "split-two-pipeline-worker-1",
                            () ->
                                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                                            restartedWorkerConfig1));
            workerNode2 =
                    startNodeWithRetry(
                            "split-two-pipeline-worker-2",
                            () ->
                                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                                            restartedWorkerConfig2));
            awaitClusterSize(masterNode1, 4);

            engineClient = createEngineClient(clusterName);
            ClientJobProxy restoredJobProxy = engineClient.createJobClient().getJobProxy(jobId);
            awaitJobStatus(restoredJobProxy, JobStatus.RUNNING);
            assertCheckpointIdsGrow(checkpointCounterStore(masterNode1), jobId, checkpointBefore);

            insertRows(tableBinding.first, 31, 32);
            insertRows(tableBinding.second, 131, 132);
            awaitSourceAndSinkConsistent(tableBinding);

            restoredJobProxy.cancelJob();
            awaitJobStatus(restoredJobProxy, JobStatus.CANCELED);
        } finally {
            closeClient(engineClient);
            shutdownNode(masterNode1);
            shutdownNode(masterNode2);
            shutdownNode(workerNode1);
            shutdownNode(workerNode2);
        }
    }

    /**
     * Creates the CDC-enabled MySQL container used by the suite.
     *
     * @return configured CDC MySQL container
     */
    private static MySqlContainer createMySqlContainer() {
        return new MySqlContainer(MySqlVersion.V8_0)
                .withConfigurationOverride("mysql-cdc-failover/docker/server-gtids/my.cnf")
                .withSetupSQL("mysql-cdc-failover/docker/setup.sql")
                .withDatabaseName("emptydb")
                .withUsername(ROOT_USERNAME)
                .withPassword(ROOT_PASSWORD);
    }

    /**
     * Builds a normal local SeaTunnel cluster configuration for multi-node failover tests.
     *
     * @param clusterName deterministic cluster name shared by every node in one scenario
     * @return local engine configuration
     */
    private SeaTunnelConfig createSeaTunnelConfig(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    /**
     * Builds a persistent local SeaTunnel cluster configuration so all-node-down recovery can
     * restore coordinator state after every member restarts.
     *
     * @param clusterName deterministic cluster name shared by every node in one scenario
     * @return local engine configuration with persistent checkpoint state
     */
    private SeaTunnelConfig createPersistentSeaTunnelConfig(String clusterName) {
        String yaml =
                "hazelcast:\n"
                        + "  cluster-name: "
                        + clusterName
                        + "\n"
                        + "  network:\n"
                        + "    rest-api:\n"
                        + "      enabled: true\n"
                        + "      endpoint-groups:\n"
                        + "        CLUSTER_WRITE:\n"
                        + "          enabled: true\n"
                        + "    join:\n"
                        + "      tcp-ip:\n"
                        + "        enabled: true\n"
                        + "        member-list:\n"
                        + "          - localhost\n"
                        + "    port:\n"
                        + "      auto-increment: true\n"
                        + "      port-count: 100\n"
                        + "      port: 5801\n"
                        + "  map:\n"
                        + "    engine*:\n"
                        + "      map-store:\n"
                        + "        enabled: true\n"
                        + "        initial-mode: EAGER\n"
                        + "        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory\n"
                        + "        properties:\n"
                        + "          type: hdfs\n"
                        + "          namespace: /tmp/seatunnel/imap\n"
                        + "          clusterName: "
                        + clusterName
                        + "\n"
                        + "          fs.defaultFS: file:///\n"
                        + "  properties:\n"
                        + "    hazelcast.invocation.max.retry.count: 200\n"
                        + "    hazelcast.tcp.join.port.try.count: 30\n"
                        + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                        + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                        + "    hazelcast.logging.type: log4j2\n"
                        + "    hazelcast.operation.generic.thread.count: 200\n";
        Config hazelcastConfig = Config.loadFromString(yaml);
        hazelcastConfig.setClusterName(clusterName);
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    /**
     * Creates a local engine client that targets the given Hazelcast cluster.
     *
     * @param clusterName deterministic cluster name shared by every node in one scenario
     * @return connected SeaTunnel client
     * @throws Exception when the client cannot be created
     */
    private SeaTunnelClient createEngineClient(String clusterName) throws Exception {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(clusterName);
        Common.setDeployMode(DeployMode.CLIENT);
        return new SeaTunnelClient(clientConfig);
    }

    /**
     * Submits the generated job config to the local engine cluster.
     *
     * @param engineClient connected engine client
     * @param seaTunnelConfig cluster configuration used to build the execution context
     * @param configPath generated job config file path
     * @param jobName user-visible job name
     * @return running job proxy
     * @throws Exception when job submission fails
     */
    private ClientJobProxy submitJob(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String configPath,
            String jobName)
            throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(configPath, jobConfig, seaTunnelConfig);
        return jobExecutionEnv.execute();
    }

    /**
     * Generates the single-pipeline job config from the CDC failover template.
     *
     * @param testCaseName stable test case name used for the generated config file
     * @param tableBinding source and sink tables for the pipeline
     * @return generated config file path
     * @throws IOException when the config file cannot be written
     */
    private String createSinglePipelineConfig(String testCaseName, TableBinding tableBinding)
            throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("dynamic_mysql_host", MYSQL_CONTAINER.getHost());
        valueMap.put("dynamic_mysql_port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        valueMap.put("dynamic_source_database", tableBinding.sourceDatabase);
        valueMap.put("dynamic_source_table", tableBinding.sourceTable);
        valueMap.put("dynamic_sink_database", tableBinding.sinkDatabase);
        valueMap.put("dynamic_sink_table", tableBinding.sinkTable);
        valueMap.put("dynamic_source_server_id", String.valueOf(nextServerId(testCaseName, 0)));
        String configPath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(SINGLE_PIPELINE_TEMPLATE, valueMap, configPath);
        return configPath;
    }

    /**
     * Generates the two-pipeline job config from the CDC failover template.
     *
     * @param testCaseName stable test case name used for the generated config file
     * @param tableBinding source and sink tables for both pipelines
     * @return generated config file path
     * @throws IOException when the config file cannot be written
     */
    private String createTwoPipelineConfig(String testCaseName, TwoTableBinding tableBinding)
            throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("dynamic_mysql_host", MYSQL_CONTAINER.getHost());
        valueMap.put("dynamic_mysql_port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        valueMap.put("dynamic_source_database_1", tableBinding.first.sourceDatabase);
        valueMap.put("dynamic_source_table_1", tableBinding.first.sourceTable);
        valueMap.put("dynamic_sink_database_1", tableBinding.first.sinkDatabase);
        valueMap.put("dynamic_sink_table_1", tableBinding.first.sinkTable);
        valueMap.put("dynamic_source_server_id_1", String.valueOf(nextServerId(testCaseName, 0)));
        valueMap.put("dynamic_source_database_2", tableBinding.second.sourceDatabase);
        valueMap.put("dynamic_source_table_2", tableBinding.second.sourceTable);
        valueMap.put("dynamic_sink_database_2", tableBinding.second.sinkDatabase);
        valueMap.put("dynamic_sink_table_2", tableBinding.second.sinkTable);
        valueMap.put("dynamic_source_server_id_2", String.valueOf(nextServerId(testCaseName, 1)));
        String configPath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(TWO_PIPELINE_TEMPLATE, valueMap, configPath);
        return configPath;
    }

    /**
     * Creates isolated source and sink tables for a single pipeline.
     *
     * @param testCaseName stable test case name used for schema naming
     * @return isolated source and sink table binding
     */
    private TableBinding createSinglePipelineBinding(String testCaseName) {
        String suffix = databaseSuffix(testCaseName);
        TableBinding tableBinding =
                new TableBinding(
                        "cdc_src_" + suffix, "cdc_sink_" + suffix, "orders_source", "orders_sink");
        createTableBinding(tableBinding);
        return tableBinding;
    }

    /**
     * Creates isolated source and sink tables for a two-pipeline job.
     *
     * @param testCaseName stable test case name used for schema naming
     * @return isolated source and sink table bindings for both pipelines
     */
    private TwoTableBinding createTwoPipelineBinding(String testCaseName) {
        String suffix = databaseSuffix(testCaseName);
        TableBinding first =
                new TableBinding(
                        "cdc_src_a_" + suffix,
                        "cdc_sink_a_" + suffix,
                        "orders_source_a",
                        "orders_sink_a");
        TableBinding second =
                new TableBinding(
                        "cdc_src_b_" + suffix,
                        "cdc_sink_b_" + suffix,
                        "orders_source_b",
                        "orders_sink_b");
        createTableBinding(first);
        createTableBinding(second);
        return new TwoTableBinding(first, second);
    }

    /**
     * Creates the backing databases and tables for one source-to-sink binding.
     *
     * @param tableBinding source and sink tables for the pipeline
     */
    private void createTableBinding(TableBinding tableBinding) {
        executeSql("CREATE DATABASE " + tableBinding.sourceDatabase);
        executeSql("CREATE DATABASE " + tableBinding.sinkDatabase);
        executeSql(
                "CREATE TABLE "
                        + tableBinding.sourceDatabase
                        + "."
                        + tableBinding.sourceTable
                        + " (id INT PRIMARY KEY, name VARCHAR(64), stage VARCHAR(64))");
        executeSql(
                "CREATE TABLE "
                        + tableBinding.sinkDatabase
                        + "."
                        + tableBinding.sinkTable
                        + " (id INT PRIMARY KEY, name VARCHAR(64), stage VARCHAR(64))");
    }

    /**
     * Inserts deterministic rows into the source table so source/sink reconciliation can compare
     * exact values by id.
     *
     * @param tableBinding source and sink tables for the pipeline
     * @param ids source row ids to insert
     */
    private void insertRows(TableBinding tableBinding, int... ids) {
        for (int id : ids) {
            executeSql(
                    "INSERT INTO "
                            + tableBinding.sourceDatabase
                            + "."
                            + tableBinding.sourceTable
                            + " (id, name, stage) VALUES ("
                            + id
                            + ", 'name-"
                            + id
                            + "', 'stage-"
                            + id
                            + "')");
        }
    }

    /**
     * Waits until the source and sink tables match exactly for one pipeline.
     *
     * @param tableBinding source and sink tables for the pipeline
     */
    private void awaitSourceAndSinkConsistent(TableBinding tableBinding) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        queryRows(
                                                tableBinding.sourceDatabase,
                                                tableBinding.sourceTable),
                                        queryRows(
                                                tableBinding.sinkDatabase,
                                                tableBinding.sinkTable)));
    }

    /**
     * Waits until both source/sink pipelines match exactly in the two-pipeline job.
     *
     * @param tableBinding source and sink tables for both pipelines
     */
    private void awaitSourceAndSinkConsistent(TwoTableBinding tableBinding) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        queryRows(
                                                                tableBinding.first.sourceDatabase,
                                                                tableBinding.first.sourceTable),
                                                        queryRows(
                                                                tableBinding.first.sinkDatabase,
                                                                tableBinding.first.sinkTable)),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        queryRows(
                                                                tableBinding.second.sourceDatabase,
                                                                tableBinding.second.sourceTable),
                                                        queryRows(
                                                                tableBinding.second.sinkDatabase,
                                                                tableBinding.second.sinkTable))));
    }

    /**
     * Waits until the given cluster member observes the expected cluster size.
     *
     * @param instance cluster member used for polling
     * @param expectedSize expected cluster member count
     */
    private void awaitClusterSize(HazelcastInstanceImpl instance, int expectedSize) {
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, instance.getCluster().getMembers().size()));
    }

    /**
     * Waits until the given job reaches the requested status.
     *
     * @param clientJobProxy job proxy used for status polling
     * @param expectedStatus expected job status
     */
    private void awaitJobStatus(ClientJobProxy clientJobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(3, TimeUnit.MINUTES)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus, clientJobProxy.getJobStatus()));
    }

    /**
     * Waits until every expected pipeline exposes a persisted checkpoint counter on the active
     * master.
     *
     * @param checkpointCounterStore checkpoint counter store on the active master
     * @param jobId target job id
     * @param expectedPipelineCount number of pipelines expected in the job
     * @return the checkpoint ids observed before failover
     */
    private Map<Integer, Long> awaitCheckpointIds(
            CounterStateStore<String> checkpointCounterStore,
            long jobId,
            int expectedPipelineCount) {
        Map<Integer, Long> checkpointBefore = new HashMap<>();
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            checkpointBefore.clear();
                            for (int pipelineId = 0;
                                    pipelineId <= expectedPipelineCount + 2;
                                    pipelineId++) {
                                String checkpointKey =
                                        StateStoreCheckpointIDCounter.convertLongIntToBase64(
                                                jobId, pipelineId);
                                Long value = checkpointCounterStore.get(checkpointKey);
                                if (value != null) {
                                    checkpointBefore.put(pipelineId, value);
                                }
                            }
                            Assertions.assertTrue(
                                    checkpointBefore.size() >= expectedPipelineCount,
                                    "Waiting for checkpoint ids before failover");
                        });
        return new HashMap<>(checkpointBefore);
    }

    /**
     * Verifies that at least one previously observed pipeline checkpoint id grows after the job
     * recovers from the injected failover.
     *
     * @param checkpointCounterStore checkpoint counter store on the active master
     * @param jobId target job id
     * @param checkpointBefore checkpoint ids captured before the failover
     */
    private void assertCheckpointIdsGrow(
            CounterStateStore<String> checkpointCounterStore,
            long jobId,
            Map<Integer, Long> checkpointBefore) {
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            boolean grew = false;
                            for (Map.Entry<Integer, Long> entry : checkpointBefore.entrySet()) {
                                String checkpointKey =
                                        StateStoreCheckpointIDCounter.convertLongIntToBase64(
                                                jobId, entry.getKey());
                                Long current = checkpointCounterStore.get(checkpointKey);
                                if (current != null && current > entry.getValue()) {
                                    grew = true;
                                    break;
                                }
                            }
                            Assertions.assertTrue(
                                    grew, "Checkpoint id should continue to grow after failover");
                        });
    }

    /**
     * Resolves the checkpoint counter store from one active cluster member.
     *
     * @param instance active cluster member
     * @return checkpoint counter store for the cluster
     */
    private CounterStateStore<String> checkpointCounterStore(HazelcastInstanceImpl instance) {
        SeaTunnelServer server =
                instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getEngineContext().getStateStores().checkpointCounterStore();
    }

    /**
     * Queries the ordered source or sink rows used by the deterministic reconciliation checks.
     *
     * @param database target database
     * @param table target table
     * @return ordered table rows
     */
    private List<List<Object>> queryRows(String database, String table) {
        String sql = "SELECT id, name, stage FROM " + database + "." + table + " ORDER BY id";
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                row.add(resultSet.getObject(1));
                row.add(resultSet.getObject(2));
                row.add(resultSet.getObject(3));
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query rows from " + database + "." + table, e);
        }
    }

    /**
     * Executes a schema or data preparation statement against the shared MySQL container.
     *
     * @param sql statement to execute
     */
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    /**
     * Opens a JDBC connection for root-level schema and data preparation.
     *
     * @return root JDBC connection
     * @throws SQLException when the connection cannot be created
     */
    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(), ROOT_USERNAME, ROOT_PASSWORD);
    }

    /**
     * Computes a deterministic server id for one MySQL CDC source within a test case.
     *
     * @param testCaseName stable test case name
     * @param offset source offset inside the job
     * @return deterministic CDC server id
     */
    private int nextServerId(String testCaseName, int offset) {
        return 6000 + Math.abs(testCaseName.hashCode() % 1000) + offset;
    }

    /**
     * Normalizes the test case name so it can safely participate in database naming.
     *
     * @param testCaseName raw test case name
     * @return lowercase alphanumeric name
     */
    private String sanitizeName(String testCaseName) {
        return testCaseName.toLowerCase().replaceAll("[^a-z0-9]+", "_");
    }

    /**
     * Builds a MySQL-safe database suffix that stays comfortably within the 64-character database
     * name limit.
     *
     * @param testCaseName raw test case name
     * @return truncated deterministic prefix plus a short unique suffix
     */
    private String databaseSuffix(String testCaseName) {
        String normalized = sanitizeName(testCaseName);
        String prefix =
                normalized.length() > 12
                        ? normalized.substring(normalized.length() - 12)
                        : normalized;
        return prefix + "_" + Long.toHexString(System.nanoTime());
    }

    /**
     * Closes a SeaTunnel client while swallowing cleanup-only failures so test assertions stay
     * focused on the failover behavior.
     *
     * @param engineClient client to close
     */
    private void closeClient(SeaTunnelClient engineClient) {
        if (engineClient != null) {
            try {
                engineClient.close();
            } catch (Exception e) {
                log.warn("Failed to close SeaTunnel client during cleanup", e);
            }
        }
    }

    /**
     * Shuts down one Hazelcast member while swallowing cleanup-only failures so test assertions
     * stay focused on the failover behavior.
     *
     * @param node cluster member to shut down
     */
    private void shutdownNode(HazelcastInstanceImpl node) {
        if (node != null) {
            try {
                node.shutdown();
            } catch (Exception e) {
                log.warn("Failed to shut down cluster node during cleanup", e);
            }
        }
    }

    /**
     * Retries one node startup until the local ports are released after an all-node-down restart.
     *
     * @param nodeName reader-friendly node name for timeout diagnostics
     * @param nodeStarter callback that creates one Hazelcast member
     * @return started cluster member
     */
    private HazelcastInstanceImpl startNodeWithRetry(String nodeName, NodeStarter nodeStarter) {
        AtomicReference<HazelcastInstanceImpl> nodeReference = new AtomicReference<>();
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(
                        () -> {
                            try {
                                nodeReference.set(nodeStarter.start());
                                return true;
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to start " + nodeName + " during restart", e);
                            }
                        });
        return nodeReference.get();
    }

    /** Encapsulates the source and sink tables for one CDC pipeline. */
    private static final class TableBinding {

        /** Source database read by the MySQL CDC connector. */
        private final String sourceDatabase;

        /** Sink database written by the JDBC sink. */
        private final String sinkDatabase;

        /** Source table name read by the MySQL CDC connector. */
        private final String sourceTable;

        /** Sink table name written by the JDBC sink. */
        private final String sinkTable;

        /**
         * Creates one source-to-sink table binding.
         *
         * @param sourceDatabase source database name
         * @param sinkDatabase sink database name
         * @param sourceTable source table name
         * @param sinkTable sink table name
         */
        private TableBinding(
                String sourceDatabase, String sinkDatabase, String sourceTable, String sinkTable) {
            this.sourceDatabase = sourceDatabase;
            this.sinkDatabase = sinkDatabase;
            this.sourceTable = sourceTable;
            this.sinkTable = sinkTable;
        }
    }

    /** Encapsulates the two source-to-sink table bindings for the multi-pipeline CDC tests. */
    private static final class TwoTableBinding {

        /** First pipeline source-to-sink table binding. */
        private final TableBinding first;

        /** Second pipeline source-to-sink table binding. */
        private final TableBinding second;

        /**
         * Creates the multi-pipeline table binding.
         *
         * @param first first pipeline source-to-sink table binding
         * @param second second pipeline source-to-sink table binding
         */
        private TwoTableBinding(TableBinding first, TableBinding second) {
            this.first = first;
            this.second = second;
        }
    }

    /** Selects which member type the test should fail in cluster scenarios. */
    private enum FailoverTarget {
        MASTER,
        WORKER
    }

    /** Creates one Hazelcast member and allows the caller to handle checked startup failures. */
    @FunctionalInterface
    private interface NodeStarter {

        /**
         * Starts one Hazelcast member for the test cluster.
         *
         * @return started Hazelcast member
         * @throws Exception when the node cannot start yet
         */
        HazelcastInstanceImpl start() throws Exception;
    }
}
