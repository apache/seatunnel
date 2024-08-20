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

package org.apache.seatunnel.e2e.connector.typesense;

import org.apache.commons.lang3.RandomUtils;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseClient;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import org.typesense.api.FieldTypes;
import org.typesense.model.Field;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class TypesenseIT extends TestSuiteBase implements TestResource {

    private static final String TYPESENSE_DOCKER_IMAGE =
            "registry.cn-hangzhou.aliyuncs.com/jast-docker/typesense:26.0";

    private static final String HOST = "e2e_typesense";

    private static final int PORT = 8108;

    private List<String> testDataset;

    private GenericContainer<?> typesenseServer;

    private TypesenseClient typesenseClient;

    private static final String sinkCollection = "typesense_test_collection";

    private static final String sourceCollection = "typesense_test_collection_for_source";

    public String createTempDataDirectory() throws IOException {
        File tmpDir = File.createTempFile("typesense_tmp_", "");
        tmpDir.delete();
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();
        return tmpDir.getPath();
    }

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        String tempDataDirectory = createTempDataDirectory();
        System.out.println(tempDataDirectory);
        typesenseServer =
                new GenericContainer<>(TYPESENSE_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withPrivilegedMode(true)
                        .withStartupAttempts(5)
                        .withCommand("--data-dir=/", "--api-key=xyz")
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(TYPESENSE_DOCKER_IMAGE)));
        typesenseServer.setPortBindings(Lists.newArrayList(String.format("%s:%s", PORT, PORT)));
        Startables.deepStart(Stream.of(typesenseServer)).join();
        log.info("Typesense container started");
        // prepare test dataset
        //        testDataset = generateTestDataSet();
        // wait for easysearch fully start
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(1L, TimeUnit.SECONDS)
                .pollInterval(1L, TimeUnit.SECONDS)
                .atMost(120L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
    }

    private void initConnection() {
        String host = typesenseServer.getContainerIpAddress();
        typesenseClient =
                TypesenseClient.createInstance(Lists.newArrayList(host + ":8108"), "xyz", "http");
    }


    /**
     * Test setting primary_keys parameter write Typesense
     */
    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testFakeToTypesenseWithPrimaryKeys(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_primary_keys.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 5);
    }

       @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testFakeToTypesenseWithRecreateSchema(TestContainer container) throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field().name("T").type(FieldTypes.BOOL));
        Assertions.assertTrue(typesenseClient.createCollection(sinkCollection,fields));
        Map<String, String> field = typesenseClient.getField(sinkCollection);
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_recreate_schema.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 5);
        Assertions.assertNotEquals(field,typesenseClient.getField(sinkCollection));
    }

    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testFakeToTypesenseWithErrorWhenNotExists(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_error_when_not_exists.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
    }

    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testFakeToTypesenseWithCreateWhenNotExists(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_create_when_not_exists.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 5);
    }

    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testFakeToTypesenseWithDropData(TestContainer container) throws Exception {
        String initData = "{\"name\":\"Han\",\"age\":12}";
        typesenseClient.createCollection(sinkCollection);
        typesenseClient.insert(sinkCollection, Lists.newArrayList(initData));
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 1);
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_drop_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 5);
    }


    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testFakeToTypesenseWithAppendData(TestContainer container) throws Exception {
        String initData = "{\"name\":\"Han\",\"age\":12}";
        typesenseClient.createCollection(sinkCollection);
        typesenseClient.insert(sinkCollection, Lists.newArrayList(initData));
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 1);
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_append_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 6);
    }

    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testFakeToTypesenseWithErrorWhenDataExists(TestContainer container) throws Exception {
        String initData = "{\"name\":\"Han\",\"age\":12}";
        typesenseClient.createCollection(sinkCollection);
        typesenseClient.insert(sinkCollection, Lists.newArrayList(initData));
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 1);
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_error_when_data_exists.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
    }

    public List<String> genTestData(int recordNum){
        ArrayList<String> testDataList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> doc = new HashMap<>();
        for (int i = 0; i < recordNum; i++) {
        try {
            doc.put("num_employees",RandomUtils.nextInt());
            doc.put("flag",RandomUtils.nextBoolean());
            doc.put("num", RandomUtils.nextLong());
            doc.put("company_name","A"+RandomUtils.nextInt(1,100));
            testDataList.add(objectMapper.writeValueAsString(doc));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        }
        return testDataList;
    }

    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_13,TestContainerId.FLINK_1_14,TestContainerId.FLINK_1_15},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Test only one engine for first change")
    @TestTemplate
    public void testTypesenseSourceAndSink(TestContainer container) throws Exception {
        int recordNum = 100;
        List<String> testData = genTestData(recordNum);
        typesenseClient.createCollection(sourceCollection);
        typesenseClient.insert(sourceCollection, testData);
        Assertions.assertEquals(typesenseClient.search(sourceCollection, null, 0).getFound(), recordNum);
        Container.ExecResult execResult =
                container.executeJob("/typesense_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), recordNum);
    }


    @AfterEach
    @Override
    public void tearDown() {
        typesenseServer.close();
    }
}
