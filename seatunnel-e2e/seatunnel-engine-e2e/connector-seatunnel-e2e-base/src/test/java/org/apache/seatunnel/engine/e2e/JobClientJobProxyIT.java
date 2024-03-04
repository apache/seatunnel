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

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

public class JobClientJobProxyIT extends SeaTunnelContainer {
    private static final String JDK_DOCKER_IMAGE = "openjdk:8";
    private static final String SERVER_SHELL = "seatunnel-cluster.sh";

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        this.server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withCommand(
                                ContainerUtil.adaptPathForWin(
                                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()))
                        .withNetworkAliases("server")
                        .withImagePullPolicy(PullPolicy.alwaysPull())
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server);
        server.setExposedPorts(Arrays.asList(5801));
        server.setPortBindings(Collections.singletonList("5801:5801"));
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        // use seatunnel_fixed_slot_num.yaml replace seatunnel.yaml in container
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel_fixed_slot_num.yaml"),
                Paths.get(SEATUNNEL_HOME, "config/seatunnel.yaml").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());
        LOG.info(
                "find images: "
                        + DockerClientFactory.lazyClient().listImagesCmd().exec().stream()
                                .map(
                                        image -> {
                                            if (image.getRepoTags() != null) {
                                                return image.getRepoTags()[0];
                                            } else {
                                                return image.getRepoDigests()[0];
                                            }
                                        })
                                .collect(Collectors.joining(",")));
        server.start();
        // execute extra commands
        executeExtraCommands(server);
    }

    @Test
    public void testJobFailedWillThrowException() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/batch_slot_not_enough.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                StringUtils.isNotBlank(execResult.getStderr())
                        && execResult
                                .getStderr()
                                .contains(
                                        "org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException: can't apply resource request"));
    }
}
