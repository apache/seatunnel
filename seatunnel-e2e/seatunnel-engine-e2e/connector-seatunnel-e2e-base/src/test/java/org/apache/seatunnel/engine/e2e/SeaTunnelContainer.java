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

import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class SeaTunnelContainer {

    private static final Logger LOG = LoggerFactory.getLogger(SeaTunnelContainer.class);
    private static final String JDK_DOCKER_IMAGE = "openjdk:8";
    protected static final Network NETWORK = Network.newNetwork();
    private static final Path PROJECT_ROOT_PATH = Paths.get(System.getProperty("user.dir")).getParent().getParent().getParent();

    private static final String SEATUNNEL_HOME = "/tmp/seatunnel";
    private static final String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";
    private static final String SEATUNNEL_BIN = Paths.get(SEATUNNEL_HOME, "bin").toString();
    private static final String SEATUNNEL_LIB = Paths.get(SEATUNNEL_HOME, "lib").toString();
    private static final String SEATUNNEL_CONNECTORS = Paths.get(SEATUNNEL_HOME, "connectors").toString();
    private static final String CLIENT_SHELL = "seatunnel.sh";
    private static final String SERVER_SHELL = "seatunnel-cluster.sh";
    private static GenericContainer<?> SERVER;

    @BeforeAll
    public static void before() {
        Map<String, String> mountMapping = getFileMountMapping();
        SERVER = new GenericContainer<>(JDK_DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withCommand(Paths.get(SEATUNNEL_BIN, SERVER_SHELL).toString())
            .withNetworkAliases("server")
            .withExposedPorts()
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .waitingFor(Wait.forLogMessage(".*received new worker register.*\\n", 1));
        mountMapping.forEach(SERVER::withFileSystemBind);
        SERVER.start();
    }

    protected static Map<String, String> getFileMountMapping() {

        Map<String, String> mountMapping = new HashMap<>();
        // copy lib
        mountMapping.put(PROJECT_ROOT_PATH + "/seatunnel-core/seatunnel-starter/target/seatunnel-starter.jar",
            Paths.get(SEATUNNEL_LIB, "seatunnel-starter.jar").toString());

        // copy bin
        mountMapping.put(PROJECT_ROOT_PATH + "/seatunnel-core/seatunnel-starter/src/main/bin/",
            Paths.get(SEATUNNEL_BIN).toString());

        // copy plugin-mapping.properties
        mountMapping.put(PROJECT_ROOT_PATH + "/plugin-mapping.properties", Paths.get(SEATUNNEL_CONNECTORS, PLUGIN_MAPPING_FILE).toString());

        return mountMapping;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public Container.ExecResult executeSeaTunnelJob(String confFile) throws IOException, InterruptedException {
        ContainerUtil.copyConnectorJarToContainer(SERVER, confFile, "seatunnel-connectors-v2", "connector-", "seatunnel", SEATUNNEL_HOME);
        final String confPath = getResource(confFile);
        if (!new File(confPath).exists()) {
            throw new IllegalArgumentException(confFile + " doesn't exist");
        }
        final String targetConfInContainer = Paths.get("/tmp", confFile).toString();
        SERVER.copyFileToContainer(MountableFile.forHostPath(confPath), targetConfInContainer);

        // Running IT use cases under Windows requires replacing \ with /
        String conf = targetConfInContainer.replaceAll("\\\\", "/");
        final List<String> command = new ArrayList<>();
        command.add(Paths.get(SEATUNNEL_BIN, CLIENT_SHELL).toString());
        command.add("--config " + conf);

        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(15000);
                // cancel server if bash command not return
                SERVER.stop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Container.ExecResult execResult = SERVER.execInContainer("bash", "-c", String.join(" ", command));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        return execResult;
    }

    @AfterAll
    public static void after() {
        if (SERVER != null) {
            SERVER.close();
        }
    }

    private String getResource(String confFile) {
        return System.getProperty("user.dir") + "/src/test/resources" + confFile;
    }

}
