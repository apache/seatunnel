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
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple3;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Cluster fault tolerance test. Test the job recovery capability and data consistency assurance
 * capability in case of cluster node failure
 */
@Slf4j
public class TextHeaderT {

    private String FILE_FORMAT_TYPE = "file_format_type";
    private String ENABLE_HEADER_WRITE = "enable_header_write";

    @Test
    public void testEnableWriteHeader() {
        List<Tuple3> lists = new ArrayList<>();
        lists.add(new Tuple3<>("text", "true", 2));
        lists.add(new Tuple3<>("text", "false", 1));
        lists.add(new Tuple3<>("csv", "true", 2));
        lists.add(new Tuple3<>("csv", "false", 1));
        lists.forEach(
                t -> {
                    try {
                        enableWriteHeader(
                                t._1().toString(),
                                t._2().toString(),
                                Integer.parseInt(t._3().toString()));
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public void enableWriteHeader(String file_format_type, String headerWrite, int lineNumber)
            throws ExecutionException, InterruptedException {
        String testClusterName = "ClusterFaultToleranceIT_EnableWriteHeaderNode";
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(headerWrite, file_format_type);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(headerWrite);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            JobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(
                                        objectCompletableFuture.isDone()
                                                && JobStatus.FINISHED.equals(
                                                        objectCompletableFuture.get()));
                            });

            Long fileLineNumberFromDir =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(lineNumber, fileLineNumberFromDir);
            log.info("========================clean test resource====================");
        } finally {
            if (engineClient != null) {
                engineClient.shutdown();
            }
            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    private ImmutablePair<String, String> createTestResources(
            @NonNull String headerWrite, @NonNull String formatType) {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(ENABLE_HEADER_WRITE, headerWrite);
        valueMap.put(FILE_FORMAT_TYPE, formatType);
        String targetDir = "/tmp/text";
        targetDir = targetDir.replace("/", File.separator);
        // clear target dir before test
        FileUtils.createNewDir(targetDir);
        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + headerWrite
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(
                "batch_fakesource_to_file_header.conf", valueMap, targetConfigFilePath);
        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }
}
