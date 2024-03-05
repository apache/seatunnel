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

package org.apache.seatunnel.engine.e2e.classloader;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.engine.e2e.SeaTunnelContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

public abstract class ClassLoaderITBase extends SeaTunnelContainer {

    private static final String CONF_FILE = "/classloader/fake_to_inmemory.conf";

    abstract boolean cacheMode();

    abstract String seatunnelConfigFileName();

    @Test
    public void testFakeSourceToInMemorySink() throws IOException, InterruptedException {
        LOG.info("test classloader with cache mode: {}", cacheMode());
        for (int i = 0; i < 10; i++) {
            // load in memory sink which already leak thread with classloader
            Container.ExecResult execResult = executeJob(server, CONF_FILE);
            Assertions.assertEquals(0, execResult.getExitCode());
            Assertions.assertTrue(containsDaemonThread());
            if (cacheMode()) {
                Assertions.assertEquals(3, getClassLoaderCount());
            } else {
                Assertions.assertEquals(2 + i, getClassLoaderCount());
            }
        }
    }

    private int getClassLoaderCount() throws IOException, InterruptedException {
        Map<String, Integer> objects = ContainerUtil.getJVMLiveObject(server);
        String className =
                "org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader";
        return objects.getOrDefault(className, 0);
    }

    private boolean containsDaemonThread() throws IOException, InterruptedException {
        List<String> threads = ContainerUtil.getJVMThreadNames(server);
        return threads.stream()
                .anyMatch(thread -> thread.contains("InMemorySinkWriter-daemon-thread"));
    }

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(
                                ContainerUtil.adaptPathForWin(
                                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()))
                        .withNetworkAliases("server")
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server);
        server.setPortBindings(Collections.singletonList("5801:5801"));

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/classloader/"
                                + seatunnelConfigFileName()),
                Paths.get(SEATUNNEL_HOME, "config", "seatunnel.yaml").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());

        server.start();
        // execute extra commands
        executeExtraCommands(server);

        File module = new File(PROJECT_ROOT_PATH + File.separator + getConnectorModulePath());
        List<File> connectorFiles =
                ContainerUtil.getConnectorFiles(
                        module, Collections.singleton("connector-fake"), getConnectorNamePrefix());
        URL url =
                FileUtils.searchJarFiles(
                                Paths.get(
                                        PROJECT_ROOT_PATH
                                                + File.separator
                                                + "seatunnel-e2e/seatunnel-e2e-common/target"))
                        .stream()
                        .filter(jar -> jar.toString().endsWith("-tests.jar"))
                        .findFirst()
                        .get();
        connectorFiles.add(new File(url.getFile()));
        connectorFiles.forEach(
                jar ->
                        server.copyFileToContainer(
                                MountableFile.forHostPath(jar.getAbsolutePath()),
                                Paths.get(SEATUNNEL_HOME, "connectors", jar.getName()).toString()));

        server.copyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/classloader/plugin-mapping.properties"),
                Paths.get(SEATUNNEL_HOME, "connectors", "plugin-mapping.properties").toString());
    }
}
