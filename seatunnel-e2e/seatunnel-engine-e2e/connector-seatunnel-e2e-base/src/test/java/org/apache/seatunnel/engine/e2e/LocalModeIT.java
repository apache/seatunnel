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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ThreadShareMode;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

public class LocalModeIT {

    SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();

    @Test
    public void localModeWithPortNotInDefaultRange() {

        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;
        try {
            Config hazelcastConfig = seaTunnelConfig.getHazelcastConfig();
            hazelcastConfig.getNetworkConfig().setPort(9999);
            SeaTunnelConfig updatedConfig = new SeaTunnelConfig();
            updatedConfig.setHazelcastConfig(hazelcastConfig);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(updatedConfig);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig
                    .getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(3000);
            Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> new SeaTunnelClient(clientConfig),
                    "Unable to connect to any cluster.");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    @Test
    public void localMode() {
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        String cluster_name = "new_cluster_name";
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Config hazelcastConfig = seaTunnelConfig.getHazelcastConfig();
            hazelcastConfig.setClusterName(cluster_name).getNetworkConfig().setPort(9999);
            SeaTunnelConfig updatedConfig = new SeaTunnelConfig();
            updatedConfig.setHazelcastConfig(hazelcastConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(updatedConfig);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(cluster_name);
            clientConfig
                    .getNetworkConfig()
                    .setAddresses(Collections.singletonList("localhost:9999"));
            engineClient = new SeaTunnelClient(clientConfig);

            Map<String, String> clusterHealthMetrics = engineClient.getClusterHealthMetrics();
            Assertions.assertEquals(1, clusterHealthMetrics.size());
            Assertions.assertTrue(clusterHealthMetrics.containsKey("[localhost]:9999"));
            Assertions.assertNotNull(engineClient.getHealthMetrics("[localhost]:9999"));
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    private static final Map<String, StartupObservation> STARTUP_OBSERVATIONS =
            new ConcurrentHashMap<>();

    @TempDir private Path checkpointDirectory;

    @ParameterizedTest
    @CsvSource({"0,ALL", "1,ALL", "0,OFF", "1,OFF"})
    void transformStartupFailureFailsEmptyAndNonemptyJobs(int rows, ThreadShareMode mode)
            throws Exception {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName("transform-startup-" + UUID.randomUUID());
        config.getEngineConfig().setTaskExecutionThreadShareMode(mode);
        config.getEngineConfig().getHttpConfig().setEnabled(false);
        config.getEngineConfig()
                .getCheckpointConfig()
                .getStorage()
                .getStoragePluginConfig()
                .put("namespace", checkpointDirectory.toString());
        config.getHazelcastConfig()
                .getNetworkConfig()
                .getInterfaces()
                .setEnabled(true)
                .addInterface("127.0.0.1");
        config.getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getMulticastConfig()
                .setEnabled(false);
        config.getHazelcastConfig().getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        List<String> observationKeys = new ArrayList<>();
        HazelcastInstanceImpl node = null;
        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(config);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(config.getHazelcastConfig().getClusterName());
            clientConfig
                    .getNetworkConfig()
                    .setAddresses(
                            Collections.singletonList(
                                    "localhost:" + node.node.getThisAddress().getPort()));
            try (SeaTunnelClient client = new SeaTunnelClient(clientConfig)) {
                runTransformStartupJob(node, client, rows, true, observationKeys);
                runTransformStartupJob(node, client, rows, false, observationKeys);
            }
        } finally {
            try {
                if (node != null) {
                    node.shutdown();
                }
            } finally {
                observationKeys.forEach(STARTUP_OBSERVATIONS::remove);
            }
        }
    }

    private void runTransformStartupJob(
            HazelcastInstanceImpl node,
            SeaTunnelClient client,
            int rows,
            boolean failOpen,
            List<String> observationKeys)
            throws Exception {
        long jobId = client.getJobClient().getNewJobId();
        String key = Long.toString(jobId);
        StartupObservation observation = new StartupObservation();
        STARTUP_OBSERVATIONS.put(key, observation);
        observationKeys.add(key);
        JobContext context = new JobContext(jobId);
        context.setJobMode(JobMode.BATCH);
        Map<String, Object> options = new HashMap<>();
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("id", "int")));
        options.put("row.num", rows);
        FakeSource source = new FakeSource(ReadonlyConfig.fromMap(options));
        source.setJobContext(context);
        CatalogTable table = source.getProducedCatalogTables().get(0);
        IdGenerator ids = new IdGenerator();
        Action upstream =
                new SourceAction<>(
                        ids.getNextId(),
                        "source",
                        source,
                        Collections.emptySet(),
                        Collections.emptySet());
        upstream.setParallelism(1);
        for (int position = 0; position < 3; position++) {
            Action transform =
                    new TransformAction(
                            ids.getNextId(),
                            "transform-" + position,
                            Collections.singletonList(upstream),
                            new StartupTransform(key, position, failOpen && position == 1, table),
                            Collections.emptySet(),
                            Collections.emptySet());
            transform.setParallelism(1);
            upstream = transform;
        }
        Action sink =
                new SinkAction<>(
                        ids.getNextId(),
                        "sink",
                        Collections.singletonList(upstream),
                        new StartupSink(key),
                        Collections.emptySet(),
                        Collections.emptySet());
        sink.setParallelism(1);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("transform-startup-" + key);
        jobConfig.setJobContext(context);
        jobConfig.getEnvOptions().put("job.retry.times", 0);
        jobConfig.getEnvOptions().put("checkpoint.interval", 1000);
        jobConfig
                .getEnvOptions()
                .put("transform_error_handler", Collections.singletonMap("mode", "LOG"));
        LogicalDag dag =
                new LogicalDagGenerator(Collections.singletonList(sink), jobConfig, ids).generate();
        ClientJobProxy job =
                client.getJobClient()
                        .createJobProxy(
                                new JobImmutableInformation(
                                        jobId,
                                        jobConfig.getName(),
                                        node.node.getNodeEngine().getSerializationService(),
                                        dag,
                                        Collections.emptyList(),
                                        Collections.emptyList()));
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        failOpen ? JobStatus.FAILED : JobStatus.FINISHED,
                                        job.getJobStatus()));
        JobResult result = job.waitForJobCompleteV2();
        if (failOpen) {
            Assertions.assertTrue(result.getError().contains("startup-denied-" + key));
        }
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(0, observation.resources.get());
                            for (AtomicInteger closes : observation.closes) {
                                Assertions.assertEquals(1, closes.get());
                            }
                        });
        Assertions.assertEquals(1, observation.opens[0].get());
        Assertions.assertEquals(1, observation.opens[1].get());
        Assertions.assertEquals(failOpen ? 0 : 1, observation.opens[2].get());
        Assertions.assertEquals(failOpen ? 0 : rows * 3, observation.maps.get());
        Assertions.assertEquals(failOpen ? 0 : rows, observation.outputs.get());
    }

    private static final class StartupObservation {
        private final AtomicInteger resources = new AtomicInteger();
        private final AtomicInteger maps = new AtomicInteger();
        private final AtomicInteger outputs = new AtomicInteger();
        private final AtomicInteger[] opens = {
            new AtomicInteger(), new AtomicInteger(), new AtomicInteger()
        };
        private final AtomicInteger[] closes = {
            new AtomicInteger(), new AtomicInteger(), new AtomicInteger()
        };
    }

    private static final class StartupTransform implements SeaTunnelMapTransform<SeaTunnelRow> {
        private final String key;
        private final int position;
        private final boolean fail;
        private final CatalogTable table;
        private transient boolean allocated;

        private StartupTransform(String key, int position, boolean fail, CatalogTable table) {
            this.key = key;
            this.position = position;
            this.fail = fail;
            this.table = table;
        }

        @Override
        public void open() {
            StartupObservation observation = STARTUP_OBSERVATIONS.get(key);
            observation.opens[position].incrementAndGet();
            observation.resources.incrementAndGet();
            allocated = true;
            if (fail) {
                throw new IllegalStateException("startup-denied-" + key);
            }
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            STARTUP_OBSERVATIONS.get(key).maps.incrementAndGet();
            return row;
        }

        @Override
        public void close() {
            StartupObservation observation = STARTUP_OBSERVATIONS.get(key);
            observation.closes[position].incrementAndGet();
            if (allocated) {
                allocated = false;
                observation.resources.decrementAndGet();
            }
        }

        @Override
        public String getPluginName() {
            return "StartupTransform";
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return table;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(table);
        }
    }

    private static final class StartupSink
            implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
        private final String key;

        private StartupSink(String key) {
            this.key = key;
        }

        @Override
        public String getPluginName() {
            return "StartupSink";
        }

        @Override
        public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
            return new SinkWriter<SeaTunnelRow, Void, Void>() {
                @Override
                public void write(SeaTunnelRow row) {
                    STARTUP_OBSERVATIONS.get(key).outputs.incrementAndGet();
                }

                @Override
                public void close() {}

                @Override
                public Optional<Void> prepareCommit() {
                    return Optional.empty();
                }

                @Override
                public void abortPrepare() {}
            };
        }
    }
}
