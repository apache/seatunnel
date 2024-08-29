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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractSeaTunnelServerTest<T extends AbstractSeaTunnelServerTest> {

    protected SeaTunnelServer server;

    protected NodeEngine nodeEngine;

    protected HazelcastInstanceImpl instance;

    protected static ILogger LOGGER;

    @BeforeAll
    public void before() {
        String name = ((T) this).getClass().getName();
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(
                TestUtils.getClusterName("AbstractSeaTunnelServerTest_" + name));
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    protected String getHazelcastConfig() {
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
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n";
    }

    public SeaTunnelConfig loadSeaTunnelConfig() {
        return ConfigProvider.locateAndGetSeaTunnelConfig();
    }

    protected void startJob(Long jobId, String path, boolean isStartWithSavePoint) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobId.toString(), jobId);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        isStartWithSavePoint,
                        nodeEngine.getSerializationService().toData(testLogicalDag),
                        testLogicalDag.getJobConfig(),
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService()
                        .submitJob(jobId, data, jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }

    @AfterAll
    public void after() {
        try {
            if (server != null) {
                server.shutdown(true);
            }

            if (instance != null) {
                instance.shutdown();
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
        }
    }

    /** For tests that require a cluster restart */
    public void restartServer() {
        this.after();
        this.before();
    }
}
