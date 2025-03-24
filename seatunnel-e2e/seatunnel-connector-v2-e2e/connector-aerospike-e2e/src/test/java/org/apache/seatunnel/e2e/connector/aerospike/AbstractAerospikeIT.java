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
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.alibaba.fastjson.JSON;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        Container.ExecResult execResult = container.executeJob("/fake_to_aerospike_sink.conf");
        validateSinkData();
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testWriteToAerospike(TestContainer container) throws Exception {
        final String testKey = "multi_type_key";
        Key key = new Key(NAMESPACE, SET_NAME, testKey);
        Map<String, Object> complexData =
                new HashMap<String, Object>() {
                    {
                        put("string_val", "seatunnel_test");
                        put("int_val", 2023);
                        put("double_val", 3.1415926);
                        put("bool_val", true);
                        put("long_val", 10000000000L);
                        put("byte_val", new byte[] {0x01, 0x02});
                        final List<String> places = new ArrayList<>();
                        places.add("a");
                        put("array_val", places);
                        put(
                                "nested_map",
                                new HashMap<String, Object>() {
                                    {
                                        put("child_str", "nested_value");
                                        put("child_int", 456);
                                    }
                                });
                    }
                };

        Bin mainBin = new Bin("complex_data", complexData);
        Bin extraBin1 = new Bin("reported", 20240601);
        Bin extraBin2 = new Bin("version", "v2.3.1");

        client.put(null, key, mainBin, extraBin1, extraBin2);

        Record record = client.get(null, key);
        Assertions.assertNotNull(record, "write records should not be empty");
        Assertions.assertEquals(3, record.bins.size(), "failed to verify the bin quantity");
    }

    @TestTemplate
    public void testReadFromAerospike(TestContainer container) throws Exception {
        testWriteToAerospike(container);
        final String testKey = "multi_type_key";
        Key key = new Key(NAMESPACE, SET_NAME, testKey);

        Record record = client.get(null, key);
        Assertions.assertNotNull(record, "no data of the specified key was queried");

        Assertions.assertEquals(20240601, ((Number) record.bins.get("reported")).intValue());
        Assertions.assertEquals("v2.3.1", record.bins.get("version"));

        Map<String, Object> data = (Map<String, Object>) record.bins.get("complex_data");

        Assertions.assertEquals("seatunnel_test", data.get("string_val"));
        Assertions.assertEquals(2023, ((Number) data.get("int_val")).intValue());
        Assertions.assertEquals(3.1415926, (Double) data.get("double_val"), 0.0001);
        Assertions.assertEquals(true, data.get("bool_val"));
        Assertions.assertEquals(10000000000L, data.get("long_val"));

        Assertions.assertArrayEquals(new byte[] {0x01, 0x02}, (byte[]) data.get("byte_val"));

        List<String> array = (List<String>) data.get("array_val");
        Assertions.assertEquals("a", array.get(0));

        Map<String, Object> nested = (Map<String, Object>) data.get("nested_map");
        Assertions.assertEquals("nested_value", nested.get("child_str"));
        Assertions.assertEquals(456, ((Number) nested.get("child_int")).intValue());
    }

    @TestTemplate
    public void testUpdateData(TestContainer container) throws Exception {
        final String testKey = "update_test_key";
        Map<String, Object> initialData = new HashMap<>();
        initialData.put("version", 1L);
        initialData.put("status", "active");
        client.put(null, new Key(NAMESPACE, SET_NAME, testKey), new Bin("data", initialData));
        Map<String, Object> updateData = new HashMap<>();
        updateData.put("version", 2L);
        updateData.put("status", "inactive");
        updateData.put("modified_time", System.currentTimeMillis());
        client.put(null, new Key(NAMESPACE, SET_NAME, testKey), new Bin("data", updateData));

        Record record = client.get(null, new Key(NAMESPACE, SET_NAME, testKey));
        Assertions.assertEquals(updateData, record.bins.get("data"), "the data update failed");
    }

    @TestTemplate
    public void testQueryByKey(TestContainer container) throws Exception {
        final int testKey = 1234;
        Map<String, Object> testData = new HashMap<>();
        testData.put("id", 1001L);
        testData.put(
                "nested",
                new HashMap<String, Object>() {
                    {
                        put("field1", "value1");
                        put("field2", 3.14);
                    }
                });
        client.put(null, new Key(NAMESPACE, SET_NAME, testKey), new Bin("data", testData));

        Record result = client.get(null, new Key(NAMESPACE, SET_NAME, testKey));

        Assertions.assertNotNull(result, "no data of the specified key was queried");
        Assertions.assertEquals(
                testData, result.bins.get("data"), "the query result data is inconsistent");

        Map<String, Object> resultData = (Map<String, Object>) result.bins.get("data");
        Map<String, Object> nested = (Map<String, Object>) resultData.get("nested");
        Assertions.assertTrue(
                nested.get("field2") instanceof Double, "nested field type is incorrect");
    }

    @TestTemplate
    public void testDeleteAll(TestContainer container) throws Exception {
        final String tempSet = "temp_delete_set";

        for (int i = 0; i < 5; i++) {
            Key key = new Key(NAMESPACE, tempSet, "key_" + i);
            client.put(null, key, new Bin("data", "test_value_" + i));
        }

        Assertions.assertDoesNotThrow(
                () -> {
                    client.scanAll(
                            null,
                            NAMESPACE,
                            tempSet,
                            (key, record) -> {
                                client.delete(null, key);
                            });
                },
                "the delete operation throws an exception");

        AtomicInteger count = new AtomicInteger();
        client.scanAll(null, NAMESPACE, tempSet, (key, record) -> count.incrementAndGet());
        Assertions.assertEquals(0, count.get(), "data deletion is not complete");
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
