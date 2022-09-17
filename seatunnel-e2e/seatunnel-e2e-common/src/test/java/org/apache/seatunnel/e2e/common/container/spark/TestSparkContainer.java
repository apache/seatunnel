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

package org.apache.seatunnel.e2e.common.container.spark;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public abstract class TestSparkContainer extends TestContainer {

    private static final Logger LOG = LoggerFactory.getLogger(TestSparkContainer.class);

    private static final String SPARK_SEATUNNEL_HOME = "/tmp/spark/seatunnel";
    private static final String DEFAULT_DOCKER_IMAGE = "bitnami/spark:2.4.3";
    public static final Network NETWORK = Network.newNetwork();

    protected GenericContainer<?> master;

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    protected String getSeaTunnelHomeInContainer() {
        return SPARK_SEATUNNEL_HOME;
    }

    @Override
    public void startUp() throws Exception {
        master = new GenericContainer<>(getDockerImage())
            .withNetwork(NETWORK)
            .withNetworkAliases("spark-master")
            .withExposedPorts()
            .withEnv("SPARK_MODE", "master")
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"));
        // In most case we can just use standalone mode to execute a spark job, if we want to use cluster mode, we need to
        // start a worker.
        Startables.deepStart(Stream.of(master)).join();
        copySeaTunnelStarter(master);
        LOG.info("Spark container started");
    }

    @Override
    public void tearDown() throws Exception {
        if (master != null) {
            master.stop();
        }
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Arrays.asList("--master local",
            "--deploy-mode client");
    }

    public Container.ExecResult executeJob(String confFile) throws IOException, InterruptedException {
        return executeJob(master, confFile);
    }
}
