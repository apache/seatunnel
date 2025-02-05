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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.ConnectorPackageClient;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class ConnectorPackageClientTest {

    protected static ILogger LOGGER;

    private static String testClusterName = "ConnectorPackageClientTest";
    private static SeaTunnelConfig SEATUNNEL_CONFIG;
    private static HazelcastInstance INSTANCE;
    private static Long JOB_ID;

    @BeforeAll
    public static void beforeClass() throws Exception {
        LOGGER = Logger.getLogger(ConnectorPackageClientTest.class);
        String yaml =
                "seatunnel:\n"
                        + "    engine:\n"
                        + "        backup-count: 1\n"
                        + "        queue-type: blockingqueue\n"
                        + "        print-execution-info-interval: 60\n"
                        + "        slot-service:\n"
                        + "            dynamic-slot: true\n"
                        + "        checkpoint:\n"
                        + "            interval: 300000\n"
                        + "            timeout: 10000\n"
                        + "            storage:\n"
                        + "                type: hdfs\n"
                        + "                max-retained: 3\n"
                        + "                plugin-config:\n"
                        + "                    namespace: /tmp/seatunnel/checkpoint_snapshot/\n"
                        + "                    storage.type: hdfs\n"
                        + "                    fs.defaultFS: file:///tmp/\n"
                        + "        jar-storage:\n"
                        + "            enable: true\n"
                        + "            connector-jar-storage-mode: SHARED\n"
                        + "            connector-jar-storage-path: \"\"\n"
                        + "            connector-jar-cleanup-task-interval: 3600\n"
                        + "            connector-jar-expiry-time: 600";

        SEATUNNEL_CONFIG = ConfigProvider.locateAndGetSeaTunnelConfigFromString(yaml);
        SEATUNNEL_CONFIG
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        INSTANCE = SeaTunnelServerStarter.createHazelcastInstance(SEATUNNEL_CONFIG);
        JOB_ID = INSTANCE.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Test
    public void testUploadCommonPluginJars() throws MalformedURLException {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
        SeaTunnelHazelcastClient seaTunnelHazelcastClient =
                new SeaTunnelHazelcastClient(clientConfig);

        String filePath = TestUtils.getResource("/client_test.conf");
        Config seaTunnelJobConfig = ConfigBuilder.of(Paths.get(filePath));
        Common.setDeployMode(DeployMode.CLIENT);
        ReadonlyConfig envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testUploadCommonPluginJars");
        jobConfig.setJobContext(new JobContext(JOB_ID));
        fillJobConfig(jobConfig, envOptions);

        ConnectorPackageClient connectorPackageClient =
                new ConnectorPackageClient(seaTunnelHazelcastClient);
        List<URL> commonPluginJars = new ArrayList<>(searchPluginJars());
        commonPluginJars.addAll(
                new ArrayList<URL>(
                        Common.getThirdPartyJars(
                                        jobConfig
                                                .getEnvOptions()
                                                .getOrDefault(EnvCommonOptions.JARS.key(), "")
                                                .toString())
                                .stream()
                                .map(Path::toUri)
                                .map(
                                        uri -> {
                                            try {
                                                return uri.toURL();
                                            } catch (MalformedURLException e) {
                                                throw new SeaTunnelEngineException(
                                                        "the uri of jar illegal:" + uri, e);
                                            }
                                        })
                                .collect(Collectors.toList())));

        if (!commonPluginJars.isEmpty()) {
            Set<ConnectorJarIdentifier> jarIdentifiers =
                    connectorPackageClient.uploadCommonPluginJars(JOB_ID, commonPluginJars);

            jarIdentifiers.forEach(
                    jarIdentifier -> {
                        await().atMost(60000, TimeUnit.MILLISECONDS)
                                .untilAsserted(
                                        () -> {
                                            Assertions.assertTrue(
                                                    StringUtils.isNotBlank(
                                                            jarIdentifier.getStoragePath()));
                                            Assertions.assertTrue(
                                                    jarIdentifier.getType()
                                                            == ConnectorJarType.COMMON_PLUGIN_JAR);
                                        });
                    });
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Test
    public void testUploadConnectorPluginJars() throws MalformedURLException {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("ConnectorPackageClientTest"));
        SeaTunnelHazelcastClient seaTunnelHazelcastClient =
                new SeaTunnelHazelcastClient(clientConfig);

        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        Config seaTunnelJobConfig = ConfigBuilder.of(Paths.get(filePath));
        ReadonlyConfig envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testUploadConnectorPluginJars");
        jobConfig.setJobContext(new JobContext(JOB_ID));
        fillJobConfig(jobConfig, envOptions);

        ConnectorPackageClient connectorPackageClient =
                new ConnectorPackageClient(seaTunnelHazelcastClient);
        Path connectorDir = Common.connectorDir();
        File[] files =
                connectorDir
                        .toFile()
                        .listFiles(
                                new FileFilter() {
                                    @Override
                                    public boolean accept(File pathname) {
                                        return pathname.getName().endsWith(".jar")
                                                && (StringUtils.startsWithIgnoreCase(
                                                                pathname.getName(),
                                                                "connector-fake")
                                                        || StringUtils.startsWithIgnoreCase(
                                                                pathname.getName(),
                                                                "connector-file"));
                                    }
                                });
        if (files != null) {
            for (File file : files) {
                ConnectorJarIdentifier connectorJarIdentifier =
                        connectorPackageClient.uploadConnectorPluginJar(
                                JOB_ID, file.toURI().toURL());
                await().atMost(60000, TimeUnit.MILLISECONDS)
                        .untilAsserted(
                                () -> {
                                    Assertions.assertTrue(
                                            StringUtils.isNotBlank(
                                                    connectorJarIdentifier.getStoragePath()));
                                    Assertions.assertTrue(
                                            connectorJarIdentifier.getType()
                                                    == ConnectorJarType.CONNECTOR_PLUGIN_JAR);
                                });
            }
        }
    }

    @Test
    public void testExecuteJob() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();

        try {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                return clientJobProxy.waitForJobComplete();
                            });

            await().atMost(180000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.FINISHED.equals(
                                                            objectCompletableFuture.get())));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @Test
    public void cancelJobTest() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();

        try {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            JobStatus jobStatus1 = clientJobProxy.getJobStatus();
            Assertions.assertFalse(jobStatus1.isEndState());
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Thread.sleep(1000);
            clientJobProxy.cancelJob();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.CANCELED.equals(
                                                            objectCompletableFuture.get())));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    private Set<URL> searchPluginJars() {
        try {
            if (Files.exists(Common.pluginRootDir())) {
                return new HashSet<>(FileUtils.searchJarFiles(Common.pluginRootDir()));
            }
        } catch (IOException | SeaTunnelEngineException e) {
            log.warn(String.format("Can't search plugin jars in %s.", Common.pluginRootDir()), e);
        }
        return Collections.emptySet();
    }

    private JobConfig fillJobConfig(JobConfig jobConfig, ReadonlyConfig envOptions) {
        jobConfig.getJobContext().setJobMode(envOptions.get(EnvCommonOptions.JOB_MODE));
        if (StringUtils.isEmpty(jobConfig.getName())
                || jobConfig.getName().equals(Constants.LOGO)) {
            jobConfig.setName(envOptions.get(EnvCommonOptions.JOB_NAME));
        }
        envOptions
                .toMap()
                .forEach(
                        (k, v) -> {
                            jobConfig.getEnvOptions().put(k, v);
                        });
        return jobConfig;
    }

    private SeaTunnelClient createSeaTunnelClient() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
        return new SeaTunnelClient(clientConfig);
    }

    @AfterAll
    public static void after() {
        INSTANCE.shutdown();
    }
}
