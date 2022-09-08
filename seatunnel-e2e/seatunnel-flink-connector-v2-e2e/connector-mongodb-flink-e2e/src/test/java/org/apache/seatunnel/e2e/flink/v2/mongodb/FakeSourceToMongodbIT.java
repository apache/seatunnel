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

package org.apache.seatunnel.e2e.flink.v2.mongodb;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;

@Slf4j
public class FakeSourceToMongodbIT extends FlinkContainer {

    private static final String MONGODB_IMAGE = "mongo:latest";

    private static final String MONGODB_CONTAINER_HOST = "flink_e2e_mongodb_sink";

    private static final int MONGODB_PORT = 27017;

    private GenericContainer<?> mongodbContainer;

    @BeforeEach
    public void startMongoContainer() {
        DockerImageName imageName = DockerImageName.parse(MONGODB_IMAGE);
        mongodbContainer = new GenericContainer<>(imageName)
            .withNetwork(NETWORK)
            .withNetworkAliases(MONGODB_CONTAINER_HOST)
            .withExposedPorts(MONGODB_PORT)
            .waitingFor(new HttpWaitStrategy()
                .forPort(MONGODB_PORT)
                .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                .withStartupTimeout(Duration.ofMinutes(2)))
            .withLogConsumer(new Slf4jLogConsumer(log));
        mongodbContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", MONGODB_PORT, MONGODB_PORT)));
        Startables.deepStart(Stream.of(mongodbContainer)).join();
        log.info("Mongodb container started");
    }

    @Test
    public void testMongodbSource() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/mongodb/fake_to_mongodb.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @AfterEach
    public void close() {
        super.close();
        if (mongodbContainer != null) {
            mongodbContainer.close();
        }
    }
}
