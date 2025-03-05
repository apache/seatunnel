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

package org.apache.seatunnel.e2e.connector.aerospike;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.alibaba.fastjson.JSON;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractAerospikeIT extends TestSuiteBase implements TestResource {

    protected static final String NAMESPACE = "test";
    protected static final String SET_NAME = "seatunnel";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String AEROSPIKE_HOST = "aerospike-host";

    protected AerospikeClient client;
    protected GenericContainer<?> container;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        container =
                new GenericContainer<>(getDockerImage())
                        .withExposedPorts(3000, 3001, 3002, 3003)
                        .withNetworkAliases(AEROSPIKE_HOST)
                        .withNetwork(NETWORK)
                        .withEnv("AEROSPIKE_NAMESPACE", NAMESPACE)
                        .withEnv("AEROSPIKE_MEM_GB", "1")
                        .withEnv("AEROSPIKE_ACCESS_ADDRESS", AEROSPIKE_HOST)
                        .withEnv("AEROSPIKE_ALTERNATE_ACCESS_ADDRESS", AEROSPIKE_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(getDockerImageName())))
                        .waitingFor(
                                Wait.forLogMessage(".*service ready: soon.*\\n", 1)
                                        .withStartupTimeout(Duration.ofMinutes(3)))
                        .withCreateContainerCmdModifier(cmd -> cmd.withHostName(AEROSPIKE_HOST));

        container.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        ClientPolicy policy = new ClientPolicy();
        policy.timeout = 30000;
        policy.failIfNotConnected = true;
        policy.readPolicyDefault.maxRetries = 10;
        policy.writePolicyDefault.maxRetries = 10;

        Host[] hosts =
                new Host[] {new Host(container.getHost(), container.getMappedPort(AEROSPIKE_PORT))};

        client = new AerospikeClient(policy, hosts);

        // Verify connection
        if (!client.isConnected()) {
            throw new IllegalStateException("Failed to connect to Aerospike server");
        }
    }

    private void insertTestData() {
        WritePolicy writePolicy = new WritePolicy();
        for (int i = 0; i < 100; i++) {
            Key key = new Key(NAMESPACE, SET_NAME, "seed_" + i);
            Bin bin1 = new Bin("id", i);
            Bin bin2 = new Bin("data", "seed-data-" + i);
            client.put(writePolicy, key, bin1, bin2);
        }
    }

    @TestTemplate
    public void testAerospikeSink(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/aerospike_sink_to_console.conf");
        validateSinkData();
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    private void validateSinkData() {
        ScanPolicy scanPolicy = new ScanPolicy();

        client.scanAll(
                scanPolicy,
                NAMESPACE,
                SET_NAME,
                (key, record) -> {
                    System.out.println("key: " + key.toString());
                    System.out.println("record: " + JSON.toJSONString(record));
                });
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (client != null) {
            client.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    abstract DockerImageName getDockerImage();

    abstract String getDockerImageName();
}
