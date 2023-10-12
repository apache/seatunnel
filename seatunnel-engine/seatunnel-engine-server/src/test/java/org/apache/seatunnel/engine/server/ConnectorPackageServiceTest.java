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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.common.utils.MDUtil;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.job.AbstractJobEnvironment;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;
import org.apache.seatunnel.engine.server.master.ConnectorPackageService;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.core.job.AbstractJobEnvironment.getJarUrlsFromIdentifiers;
import static org.awaitility.Awaitility.await;

public class ConnectorPackageServiceTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testMasterNodeActive() {
        HazelcastInstanceImpl instance1 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "ConnectorPackageServiceTest_testMasterNodeActive"));
        HazelcastInstanceImpl instance2 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "ConnectorPackageServiceTest_testMasterNodeActive"));

        SeaTunnelServer server1 =
                instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        SeaTunnelServer server2 =
                instance2.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        Assertions.assertTrue(server1.isMasterNode());
        ConnectorPackageService connectorPackageService1 = server1.getConnectorPackageService();
        Assertions.assertTrue(connectorPackageService1.isConnectorPackageServiceActive());

        try {
            server2.getConnectorPackageService();
            Assertions.fail("Need throw SeaTunnelEngineException here but not.");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof SeaTunnelEngineException);
        }

        // shutdown instance1
        instance1.shutdown();
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            try {
                                Assertions.assertTrue(server2.isMasterNode());
                                ConnectorPackageService connectorPackageService =
                                        server2.getConnectorPackageService();
                                Assertions.assertTrue(
                                        connectorPackageService.isConnectorPackageServiceActive());
                            } catch (SeaTunnelEngineException e) {
                                Assertions.assertTrue(false);
                            }
                        });
        instance2.shutdown();
    }

    @Test
    public void testRestoreWhenMasterNodeSwitch() throws InterruptedException, IOException {
        HazelcastInstanceImpl instance1 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "ConnectorPackageServiceTest_testJobRestoreWhenMasterNodeSwitch"));
        HazelcastInstanceImpl instance2 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "ConnectorPackageServiceTest_testJobRestoreWhenMasterNodeSwitch"));

        SeaTunnelServer server1 =
                instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        SeaTunnelServer server2 =
                instance2.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        CoordinatorService coordinatorService = server1.getCoordinatorService();
        Assertions.assertTrue(coordinatorService.isCoordinatorActive());

        ConnectorPackageService connectorPackageService = server1.getConnectorPackageService();
        Assertions.assertTrue(connectorPackageService.isConnectorPackageServiceActive());

        Long jobId = instance1.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        LogicalDag testLogicalDag =
                TestUtils.createTestLogicalPlan(
                        "stream_fakesource_to_file.conf",
                        "testJobRestoreWhenMasterNodeSwitch",
                        jobId);

        String filePath = TestUtils.getResource("stream_fakesource_to_file.conf");
        Config seaTunnelJobConfig = ConfigBuilder.of(Paths.get(filePath));
        ReadonlyConfig envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testRestoreWhenMasterNodeSwitch");
        jobConfig.setJobContext(new JobContext(jobId));
        fillJobConfig(jobConfig, envOptions);
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
        MultipleTableJobConfigParser multipleTableJobConfigParser =
                new MultipleTableJobConfigParser(
                        filePath, new IdGenerator(), jobConfig, commonPluginJars, false);
        ImmutablePair<List<Action>, Set<URL>> immutablePair = multipleTableJobConfigParser.parse();
        Set<ConnectorJarIdentifier> commonJarIdentifiers = new HashSet<>();

        // Upload commonPluginJar
        for (URL commonPluginJar : commonPluginJars) {
            // handle the local file path
            // origin path : /${SEATUNNEL_HOME}/plugins/Jdbc/lib/mysql-connector-java-5.1.32.jar ->
            // handled path : ${SEATUNNEL_HOME}/plugins/Jdbc/lib/mysql-connector-java-5.1.32.jar
            Path path = Paths.get(commonPluginJar.getPath().substring(1));
            // Obtain the directory name of the relative location of the file path.
            // for example, The path is
            // ${SEATUNNEL_HOME}/plugins/Jdbc/lib/mysql-connector-java-5.1.32.jar, so the name
            // obtained here is the connector plugin name : JDBC
            int directoryIndex = path.getNameCount() - 3;
            String pluginName = path.getName(directoryIndex).toString();
            byte[] data = readFileData(path);
            String fileName = getFileNameFromURL(commonPluginJar);

            // compute the digest of the file
            MessageDigest messageDigest = MDUtil.createMessageDigest();
            byte[] digest = messageDigest.digest(data);

            ConnectorJar connectorJar =
                    ConnectorJar.createConnectorJar(
                            digest, ConnectorJarType.COMMON_PLUGIN_JAR, data, pluginName, fileName);
            ConnectorJarIdentifier commonJarIdentifier =
                    connectorPackageService.storageConnectorJarFile(
                            jobId, nodeEngine.getSerializationService().toData(connectorJar));
            commonJarIdentifiers.add(commonJarIdentifier);
        }

        Set<URL> commonPluginJarUrls = getJarUrlsFromIdentifiers(commonJarIdentifiers);
        Set<ConnectorJarIdentifier> pluginJarIdentifiers = new HashSet<>();
        transformActionPluginJarUrls(
                immutablePair.getLeft(), pluginJarIdentifiers, jobId, connectorPackageService);
        Set<URL> connectorPluginJarUrls = getJarUrlsFromIdentifiers(pluginJarIdentifiers);
        List<ConnectorJarIdentifier> connectorJarIdentifiers = new ArrayList<>();
        List<URL> jarUrls = new ArrayList<>();
        connectorJarIdentifiers.addAll(commonJarIdentifiers);
        connectorJarIdentifiers.addAll(pluginJarIdentifiers);
        jarUrls.addAll(commonPluginJarUrls);
        jarUrls.addAll(connectorPluginJarUrls);
        List<Action> actions = immutablePair.getLeft();
        actions.forEach(
                action -> {
                    AbstractJobEnvironment.addCommonPluginJarsToAction(
                            action, commonPluginJarUrls, commonJarIdentifiers);
                });
        LogicalDagGenerator logicalDagGenerator =
                new LogicalDagGenerator(actions, jobConfig, new IdGenerator());
        LogicalDag logicalDag = logicalDagGenerator.generate();

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        instance1.getSerializationService().toData(logicalDag),
                        logicalDag.getJobConfig(),
                        jarUrls,
                        connectorJarIdentifiers);

        Data data = instance1.getSerializationService().toData(jobImmutableInformation);

        coordinatorService.submitJob(jobId, data).join();

        // waiting for job status turn to running
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, coordinatorService.getJobStatus(jobId)));

        // test master node shutdown
        instance1.shutdown();
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            try {
                                Assertions.assertTrue(server2.isMasterNode());
                                Assertions.assertTrue(
                                        server2.getCoordinatorService().isCoordinatorActive());
                            } catch (SeaTunnelEngineException e) {
                                Assertions.assertTrue(false);
                            }
                        });

        // pipeline will leave running state
        await().atMost(200000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNotEquals(
                                        PipelineStatus.RUNNING,
                                        server2.getCoordinatorService()
                                                .getJobMaster(jobId)
                                                .getPhysicalPlan()
                                                .getPipelineList()
                                                .get(0)
                                                .getPipelineState()));

        // pipeline will recovery running state
        await().atMost(200000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING,
                                        server2.getCoordinatorService()
                                                .getJobMaster(jobId)
                                                .getPhysicalPlan()
                                                .getPipelineList()
                                                .get(0)
                                                .getPipelineState()));

        server2.getCoordinatorService().cancelJob(jobId);

        // because runningJobMasterMap is empty and we have no JobHistoryServer, so return
        await().atMost(200000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        server2.getCoordinatorService().getJobStatus(jobId)));
        instance2.shutdown();
    }

    private static String getFileNameFromURL(URL url) {
        String path = url.getPath();
        String[] segments = path.split("/");
        return segments[segments.length - 1];
    }

    private Set<URL> searchPluginJars() {
        try {
            if (Files.exists(Common.pluginRootDir())) {
                return new HashSet<>(FileUtils.searchJarFiles(Common.pluginRootDir()));
            }
        } catch (IOException | SeaTunnelEngineException e) {
            LOGGER.warning(
                    String.format("Can't search plugin jars in %s.", Common.pluginRootDir()), e);
        }
        return Collections.emptySet();
    }

    private Set<ConnectorJarIdentifier> uploadPluginJarUrls(
            Long jobId, Set<URL> pluginJarUrls, ConnectorPackageService connectorPackageService) {
        Set<ConnectorJarIdentifier> pluginJarIdentifiers = new HashSet<>();
        pluginJarUrls.forEach(
                pluginJarUrl -> {
                    Path connectorPluginJarPath = Paths.get(pluginJarUrl.getPath().substring(1));

                    byte[] data = readFileData(connectorPluginJarPath);
                    String fileName = connectorPluginJarPath.getFileName().toString();

                    // compute the digest of the file
                    MessageDigest messageDigest = MDUtil.createMessageDigest();
                    byte[] digest = messageDigest.digest(data);

                    ConnectorJar connectorJar =
                            ConnectorJar.createConnectorJar(
                                    digest, ConnectorJarType.CONNECTOR_PLUGIN_JAR, data, fileName);
                    ConnectorJarIdentifier connectorJarIdentifier =
                            connectorPackageService.storageConnectorJarFile(
                                    jobId,
                                    nodeEngine.getSerializationService().toData(connectorJar));
                    pluginJarIdentifiers.add(connectorJarIdentifier);
                });
        return pluginJarIdentifiers;
    }

    private void transformActionPluginJarUrls(
            List<Action> actions,
            Set<ConnectorJarIdentifier> result,
            Long jobId,
            ConnectorPackageService connectorPackageService) {
        actions.forEach(
                action -> {
                    Set<URL> jarUrls = action.getJarUrls();
                    Set<ConnectorJarIdentifier> jarIdentifiers =
                            uploadPluginJarUrls(jobId, jarUrls, connectorPackageService);
                    result.addAll(jarIdentifiers);
                    // Reset the client URL of the jar package in Set
                    // add the URLs from remote master node
                    jarUrls.clear();
                    jarUrls.addAll(getJarUrlsFromIdentifiers(jarIdentifiers));
                    action.getConnectorJarIdentifiers().addAll(jarIdentifiers);
                    if (!action.getUpstream().isEmpty()) {
                        transformActionPluginJarUrls(
                                action.getUpstream(), result, jobId, connectorPackageService);
                    }
                });
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

    private static byte[] readFileData(Path filePath) {
        // Read file data and convert it to a byte array.
        try {
            InputStream inputStream = Files.newInputStream(filePath);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            LOGGER.warning(
                    String.format(
                            "Failed to read the connector jar package file : { %s } , the file to be read may not exist",
                            filePath.toString()));
            throw new RuntimeException();
        }
    }
}
