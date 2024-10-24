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

package org.apache.seatunnel.engine.e2e.telemetry;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryMetricConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.restassured.response.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.matchesRegex;

@Slf4j
public abstract class AbstractTelemetryBaseIT {

    private static final String HOST = "http://localhost:";

    private SeaTunnelClient engineClient;

    public abstract void open(SeaTunnelConfig... seaTunnelConfigs) throws Exception;

    public abstract void close() throws Exception;

    public abstract int getNodeCount();

    public abstract String getClusterName();

    public abstract void testGetMetrics() throws Exception;

    @BeforeEach
    public void before() throws Exception {
        int nodeCount = getNodeCount();
        SeaTunnelConfig[] seaTunnelConfigs = new SeaTunnelConfig[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(getClusterName());
            seaTunnelConfigs[i] = seaTunnelConfig;
        }
        this.open(seaTunnelConfigs);
        runJob(getSeaTunnelConfig(getClusterName()), getClusterName());
    }

    @AfterEach
    public void after() throws Exception {
        engineClient.close();
        this.close();
    }

    public void testGetMetrics(HazelcastInstanceImpl hazelcastInstance, String testClusterName)
            throws InterruptedException {
        Response response =
                given().get(
                                HOST
                                        + hazelcastInstance
                                                .getCluster()
                                                .getLocalMember()
                                                .getAddress()
                                                .getPort()
                                        + "/hazelcast/rest/instance/metrics");
        response.then()
                .statusCode(200)
                // Use regular expressions to verify whether the response body is the indicator data
                // of Prometheus
                // Metric data is usually multi-line, use newlines for validation
                .body(matchesRegex("(?s)^.*# HELP.*# TYPE.*$"))
                // Verify that the response body contains a specific metric
                // JVM metrics
                .body(containsString("jvm_threads"))
                .body(containsString("jvm_memory_pool"))
                .body(containsString("jvm_gc"))
                .body(containsString("jvm_info"))
                .body(containsString("jvm_memory_bytes"))
                .body(containsString("jvm_classes"))
                .body(containsString("jvm_buffer_pool"))
                .body(containsString("process_start"))
                // cluster_info
                .body(containsString("cluster_info{cluster=\"" + testClusterName))
                // cluster_time
                .body(containsString("cluster_time{cluster=\"" + testClusterName))
                // Job thread pool metrics
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_activeCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_completedTask_total\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_corePoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_maximumPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_task_total\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_queueTaskCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*job_thread_pool_rejection_total\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // Job count metrics
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"canceled\",} 0.0"))
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"cancelling\",} 0.0"))
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"created\",} 0.0"))
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"failed\",} 0.0"))
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"failing\",} 0.0"))
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"finished\",} 0.0"))
                // Running job count is 1
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"running\",} 1.0"))
                .body(
                        containsString(
                                "job_count{cluster=\""
                                        + testClusterName
                                        + "\",type=\"scheduled\",} 0.0"))
                // Node
                .body(
                        matchesRegex(
                                "(?s)^.*node_state\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_executor_executedCount
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))
                // hazelcast_executor_isShutdown

                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_isTerminated
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_maxPoolSize
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_poolSize
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_queueRemainingCapacity
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_queueSize
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_partition_partitionCount
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_partitionCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_partition_activePartition
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_activePartition\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_partition_isClusterSafe
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_isClusterSafe\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_partition_isLocalMemberSafe
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_isLocalMemberSafe\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"));
    }

    public SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(testClusterName);
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        TelemetryMetricConfig telemetryMetricConfig = new TelemetryMetricConfig();
        telemetryMetricConfig.setEnabled(true);
        TelemetryConfig telemetryConfig = new TelemetryConfig();
        telemetryConfig.setMetric(telemetryMetricConfig);
        seaTunnelConfig.getEngineConfig().setTelemetryConfig(telemetryConfig);
        return seaTunnelConfig;
    }

    private static String getHazelcastConfig() {
        return "hazelcast:\n"
                + "  cluster-name: seatunnel\n"
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
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n";
    }

    public void runJob(SeaTunnelConfig seaTunnelConfig, String clusterName) throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("stream_fakesource_to_console.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(clusterName);
        engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

        final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus()));
    }
}
