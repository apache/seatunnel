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

package org.apache.seatunnel.e2e.common;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class is the base class of FlinkEnvironment test.
 * The before method will create a Flink cluster, and after method will close the Flink cluster.
 * You can use {@link AbstractFlinkContainer#executeSeaTunnelFlinkJob} to submit a seatunnel config and run a seatunnel job.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractFlinkContainer extends AbstractContainer {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkContainer.class);

    protected static final String FLINK_SEATUNNEL_HOME = "/tmp/flink/seatunnel";

    protected static final Network NETWORK = Network.newNetwork();

    protected static final List<String> DEFAULT_FLINK_PROPERTIES = Arrays.asList(
        "jobmanager.rpc.address: jobmanager",
        "taskmanager.numberOfTaskSlots: 10",
        "parallelism.default: 4",
        "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false");

    protected static final String DEFAULT_DOCKER_IMAGE = "flink:1.13.6-scala_2.11";

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    protected String getSeaTunnelHomeInContainer() {
        return FLINK_SEATUNNEL_HOME;
    }

    @BeforeAll
    public void before() {
        final String dockerImage = getDockerImage();
        final String properties = String.join("\n", getFlinkProperties());
        jobManager = new GenericContainer<>(dockerImage)
            .withCommand("jobmanager")
            .withNetwork(NETWORK)
            .withNetworkAliases("jobmanager")
            .withExposedPorts()
            .withEnv("FLINK_PROPERTIES", properties)
            .withLogConsumer(new Slf4jLogConsumer(LOG));

        taskManager =
            new GenericContainer<>(dockerImage)
                .withCommand("taskmanager")
                .withNetwork(NETWORK)
                .withNetworkAliases("taskmanager")
                .withEnv("FLINK_PROPERTIES", properties)
                .dependsOn(jobManager)
                .withLogConsumer(new Slf4jLogConsumer(LOG));

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        copySeaTunnelStarter(jobManager);
        LOG.info("Flink containers are started.");
    }

    protected List<String> getFlinkProperties() {
        return DEFAULT_FLINK_PROPERTIES;
    }

    @AfterAll
    public void close() {
        if (taskManager != null) {
            taskManager.stop();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Collections.emptyList();
    }

    public Container.ExecResult executeSeaTunnelFlinkJob(String confFile) throws IOException, InterruptedException {
        return executeJob(jobManager, confFile);
    }
}
