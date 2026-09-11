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

import org.apache.seatunnel.api.cdc.CdcEnumeratorProgressReport;
import org.apache.seatunnel.api.cdc.CdcProgressAccuracy;
import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.api.cdc.CdcSnapshotAssignmentStatus;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressEnvelope;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressOwner;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressService;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Validates internal CDC reports against a real MySQL job on an embedded split Zeta cluster. */
@Slf4j
public class MysqlCdcProgressIT {

    @TempDir Path temporaryDirectory;

    private final DeployMode originalDeployMode = Common.getDeployMode();
    private MySQLContainer<?> mysql;
    private HazelcastInstanceImpl master;
    private HazelcastInstanceImpl worker;
    private SeaTunnelClient client;

    @Test
    void reportsSnapshotCompletionAndIncrementalChangesFromTheWorker() throws Exception {
        mysql =
                new MySQLContainer<>("mysql:8.0.36")
                        .withDatabaseName("cdc_progress")
                        .withUsername("root")
                        .withPassword("mysqlpw")
                        .withEnv("MYSQL_ROOT_HOST", "%")
                        .withCommand(
                                "--server-id=1",
                                "--log-bin=mysql-bin",
                                "--binlog-format=ROW",
                                "--binlog-row-image=FULL");
        mysql.start();
        executeSql(
                "CREATE TABLE source_rows (id INT PRIMARY KEY, payload VARCHAR(64))",
                "CREATE TABLE sink_rows (id INT PRIMARY KEY, payload VARCHAR(64))",
                "INSERT INTO source_rows VALUES (1, 'snapshot-one'), (2, 'snapshot-two')");

        String clusterName = "mysql-cdc-progress-" + temporaryDirectory.getFileName();
        SeaTunnelConfig masterConfig = createServerConfig(clusterName);
        master = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
        Address masterAddress = master.getCluster().getLocalMember().getAddress();
        String masterEndpoint = masterAddress.getHost() + ":" + masterAddress.getPort();
        SeaTunnelConfig workerConfig = createServerConfig(clusterName);
        workerConfig
                .getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .setMembers(Collections.singletonList(masterEndpoint));
        worker = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> Assertions.assertEquals(2, master.getCluster().getMembers().size()));

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.getNetworkConfig().setAddresses(Collections.singletonList(masterEndpoint));
        client = new SeaTunnelClient(clientConfig);
        Common.setDeployMode(DeployMode.CLUSTER);
        Path jobFile = temporaryDirectory.resolve("mysql-cdc-progress.conf");
        TestUtils.createTestConfigFileFromTemplate(
                "mysql_cdc_progress_template.conf",
                Collections.singletonMap("dynamic_jdbc_url", mysql.getJdbcUrl()),
                jobFile.toString());
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("mysql-cdc-progress");
        ClientJobProxy job =
                client.createExecutionContext(jobFile.toString(), jobConfig, masterConfig)
                        .execute();
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.RUNNING, job.getJobStatus()));

        SeaTunnelServer server =
                master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        List<SubPlan> pipelines =
                server.getCoordinatorService()
                        .getJobMaster(job.getJobId())
                        .getPhysicalPlan()
                        .getPipelineList();
        Assertions.assertEquals(1, pipelines.size());
        SubPlan pipeline = pipelines.get(0);
        List<PhysicalVertex> sourceCoordinators =
                pipeline.getCoordinatorVertexList().stream()
                        .filter(
                                vertex ->
                                        vertex.getTaskGroup().getTasks().stream()
                                                .anyMatch(
                                                        SourceSplitEnumeratorTask.class
                                                                ::isInstance))
                        .collect(Collectors.toList());
        Assertions.assertEquals(1, sourceCoordinators.size());
        PhysicalVertex sourceCoordinator = sourceCoordinators.get(0);
        SourceSplitEnumeratorTask<?> enumeratorTask =
                (SourceSplitEnumeratorTask<?>)
                        sourceCoordinator.getTaskGroup().getTasks().stream()
                                .filter(SourceSplitEnumeratorTask.class::isInstance)
                                .findFirst()
                                .get();
        long sourceVertexId = enumeratorTask.getCdcProgressSourceVertexId();
        int pipelineId = pipeline.getPipelineId();
        long jobId = job.getJobId();
        CdcProgressService progress = server.getCdcProgressService();
        Assertions.assertEquals(
                worker.getCluster().getLocalMember().getAddress(),
                sourceCoordinator.getCurrentExecutionAddress(),
                "The enumerator must be hosted away from the master to exercise collection transport");

        log.info("Waiting for snapshot rows and enumerator completion for job {}", jobId);
        awaitRows(rowMap(1, "snapshot-one", 2, "snapshot-two"));
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                assertSnapshotCompleted(
                                        progress.getEnumeratorReport(
                                                jobId, pipelineId, sourceVertexId)));

        executeSql("INSERT INTO source_rows VALUES (3, 'incremental-three')");
        awaitRows(rowMap(1, "snapshot-one", 2, "snapshot-two", 3, "incremental-three"));
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                assertIncremental(
                                        readerReport(progress, jobId, pipelineId, sourceVertexId)));
        CdcProgressEnvelope<CdcReaderProgressReport> before =
                readerReport(progress, jobId, pipelineId, sourceVertexId);
        Map<String, String> beforePosition =
                before.getReport().getCurrentConsumedPosition().getValue().getValues();
        BinlogOffset batchStart = currentBinlogEnd();
        Assertions.assertTrue(
                new BinlogOffset(beforePosition).isBefore(batchStart),
                "The initial report must precede the next batch's binlog boundary");

        log.info("Checking insert, update, and delete with incremental progress for job {}", jobId);
        executeSql(
                "UPDATE source_rows SET payload = 'updated-one' WHERE id = 1",
                "DELETE FROM source_rows WHERE id = 2",
                "INSERT INTO source_rows VALUES (4, 'incremental-four')");
        awaitRows(rowMap(1, "updated-one", 3, "incremental-three", 4, "incremental-four"));
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            CdcProgressEnvelope<CdcReaderProgressReport> after =
                                    readerReport(progress, jobId, pipelineId, sourceVertexId);
                            assertIncremental(after);
                            Assertions.assertEquals(
                                    before.getExecutionAttemptId(), after.getExecutionAttemptId());
                            Assertions.assertTrue(
                                    after.getReportSequence() > before.getReportSequence());
                            Assertions.assertNotEquals(
                                    beforePosition,
                                    after.getReport()
                                            .getCurrentConsumedPosition()
                                            .getValue()
                                            .getValues());
                            // A delayed report from the first INSERT cannot cross this boundary.
                            // Compare native coordinates, not sampling time or report sequence
                            // alone.
                            Assertions.assertTrue(
                                    new BinlogOffset(
                                                    after.getReport()
                                                            .getCurrentConsumedPosition()
                                                            .getValue()
                                                            .getValues())
                                            .isAtOrAfter(batchStart),
                                    "Consumed progress must reach a record from the final DML batch");
                            Assertions.assertTrue(
                                    after.getReport().getLastPositionChangeAt()
                                            >= before.getReport().getLastPositionChangeAt());
                        });

        job.cancelJob();
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.CANCELED, job.getJobStatus()));
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    progress.getReaderReports(jobId, pipelineId, sourceVertexId)
                                            .isEmpty());
                            Assertions.assertNull(
                                    progress.getEnumeratorReport(
                                            jobId, pipelineId, sourceVertexId));
                        });
    }

    private SeaTunnelConfig createServerConfig(String clusterName) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(clusterName);
        NetworkConfig network = config.getHazelcastConfig().getNetworkConfig();
        network.setPort(0);
        network.getJoin().getMulticastConfig().setEnabled(false);
        network.getJoin().getTcpIpConfig().setEnabled(false);
        network.getRestApiConfig().setEnabled(false);
        config.getEngineConfig().setBackupCount(0);
        config.getEngineConfig().getHttpConfig().setEnabled(false);
        config.getEngineConfig()
                .getCheckpointConfig()
                .getStorage()
                .getStoragePluginConfig()
                .put("namespace", temporaryDirectory.resolve("checkpoints").toString());
        return config;
    }

    private static void assertSnapshotCompleted(
            CdcProgressEnvelope<CdcEnumeratorProgressReport> envelope) {
        Assertions.assertNotNull(envelope, "No enumerator report reached the master");
        Assertions.assertEquals(CdcProgressOwner.ENUMERATOR, envelope.getOwner());
        CdcEnumeratorProgressReport report = envelope.getReport();
        Assertions.assertEquals("MySQL-CDC", report.getConnectorType());
        Assertions.assertEquals(
                CdcSnapshotAssignmentStatus.COMPLETED, report.getSnapshotAssignmentStatus());
        Assertions.assertEquals(
                CdcProgressAccuracy.EXACT, report.getAssignedSplitCount().getAccuracy());
        Assertions.assertEquals(
                CdcProgressAccuracy.EXACT, report.getCompletedSplitCount().getAccuracy());
        // Completed snapshot metadata may already have been pruned after the reader's phase event.
        // These are current retained counts, not a historical total of all snapshot splits.
        Assertions.assertEquals(
                report.getAssignedSplitCount().getValue(),
                report.getCompletedSplitCount().getValue());
        Assertions.assertEquals(Integer.valueOf(0), report.getRunningSplitCount().getValue());
        Assertions.assertTrue(report.getActiveSplits().isEmpty());
    }

    private static CdcProgressEnvelope<CdcReaderProgressReport> readerReport(
            CdcProgressService progress, long jobId, int pipelineId, long sourceVertexId) {
        List<CdcProgressEnvelope<CdcReaderProgressReport>> reports =
                progress.getReaderReports(jobId, pipelineId, sourceVertexId);
        Assertions.assertEquals(
                1, reports.size(), "Expected the single reader's report at the master");
        return reports.get(0);
    }

    private static void assertIncremental(CdcProgressEnvelope<CdcReaderProgressReport> envelope) {
        Assertions.assertEquals(CdcProgressOwner.READER, envelope.getOwner());
        CdcReaderProgressReport report = envelope.getReport();
        Assertions.assertEquals("MySQL-CDC", report.getConnectorType());
        Assertions.assertEquals(CdcProgressLifecycle.INCREMENTAL, report.getLifecycle());
        Assertions.assertEquals(
                CdcProgressAccuracy.EXACT, report.getCurrentConsumedPosition().getAccuracy());
        CdcProgressPosition position = report.getCurrentConsumedPosition().getValue();
        Assertions.assertEquals("MYSQL_BINLOG", position.getType());
        Assertions.assertNotNull(position.getValues().get("file"));
        Assertions.assertTrue(Long.parseLong(position.getValues().get("pos")) > 0);
        Assertions.assertTrue(report.getLastPositionChangeAt() > 0);
        Assertions.assertEquals(
                CdcProgressAccuracy.UNSUPPORTED,
                report.getLastCompletedCheckpointPosition().getAccuracy());
        Assertions.assertNull(report.getLastCompletedCheckpointPosition().getValue());
        Assertions.assertEquals(
                CdcProgressAccuracy.UNSUPPORTED, report.getRestoredPosition().getAccuracy());
        Assertions.assertNull(report.getRestoredPosition().getValue());
    }

    private void awaitRows(Map<Integer, String> expected) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(expected, readRows("source_rows"));
                            Assertions.assertEquals(expected, readRows("sink_rows"));
                        });
    }

    private Map<Integer, String> readRows(String table) throws Exception {
        Map<Integer, String> rows = new TreeMap<>();
        try (Connection connection = connection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT id, payload FROM " + table)) {
            while (result.next()) {
                rows.put(result.getInt(1), result.getString(2));
            }
        }
        return rows;
    }

    private void executeSql(String... statements) throws Exception {
        try (Connection connection = connection();
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private BinlogOffset currentBinlogEnd() throws Exception {
        try (Connection connection = connection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SHOW MASTER STATUS")) {
            Assertions.assertTrue(result.next(), "MySQL must expose its current binary log");
            // This pinned single-server fixture uses file/position coordinates; omitting GTIDs
            // keeps the comparison against the log boundary, not transaction-set containment.
            return new BinlogOffset(result.getString("File"), result.getLong("Position"));
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    private static Map<Integer, String> rowMap(Object... values) {
        Map<Integer, String> rows = new TreeMap<>();
        for (int i = 0; i < values.length; i += 2) {
            rows.put((Integer) values[i], (String) values[i + 1]);
        }
        return rows;
    }

    @AfterEach
    void closeResources() {
        try {
            Assertions.assertAll(
                    Arrays.asList(
                            () -> {
                                if (client != null) {
                                    client.close();
                                }
                            },
                            () -> {
                                if (worker != null) {
                                    worker.shutdown();
                                }
                            },
                            () -> {
                                if (master != null) {
                                    master.shutdown();
                                }
                            },
                            () -> {
                                if (mysql != null) {
                                    mysql.close();
                                }
                            }));
        } finally {
            Common.setDeployMode(originalDeployMode);
        }
    }
}
