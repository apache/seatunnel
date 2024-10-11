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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.typesense.catalog.TypesenseCatalog;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseClient;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.commons.lang3.RandomUtils;

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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class TypesenseIT extends TestSuiteBase implements TestResource {

    private static final String TYPESENSE_DOCKER_IMAGE = "typesense/typesense:26.0";

    private static final String HOST = "e2e_typesense";

    private static final int PORT = 8108;

    private GenericContainer<?> typesenseServer;

    private TypesenseClient typesenseClient;

    private static final String sinkCollection = "typesense_test_collection";

    private static final String sourceCollection = "typesense_test_collection_for_source";

    private Catalog catalog;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
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
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(1L, TimeUnit.SECONDS)
                .pollInterval(1L, TimeUnit.SECONDS)
                .atMost(120L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
    }

    private void initConnection() {
        String host = typesenseServer.getContainerIpAddress();
        Map<String, Object> config = new HashMap<>();
        config.put(TypesenseConnectionConfig.HOSTS.key(), Lists.newArrayList(host + ":8108"));
        config.put(TypesenseConnectionConfig.APIKEY.key(), "xyz");
        config.put(TypesenseConnectionConfig.PROTOCOL.key(), "http");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        typesenseClient = TypesenseClient.createInstance(readonlyConfig);
        catalog = new TypesenseCatalog("ty", "", readonlyConfig);
        catalog.open();
    }

    /** Test setting primary_keys parameter write Typesense */
    @TestTemplate
    public void testFakeToTypesenseWithPrimaryKeys(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_primary_keys.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 5);
    }

    @TestTemplate
    public void testFakeToTypesenseWithRecreateSchema(TestContainer container) throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field().name("T").type(FieldTypes.BOOL));
        Assertions.assertTrue(typesenseClient.createCollection(sinkCollection, fields));
        Map<String, String> field = typesenseClient.getField(sinkCollection);
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_recreate_schema.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 5);
        Assertions.assertNotEquals(field, typesenseClient.getField(sinkCollection));
    }

    @TestTemplate
    public void testFakeToTypesenseWithErrorWhenNotExists(TestContainer container)
            throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_error_when_not_exists.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
    }

    @TestTemplate
    public void testFakeToTypesenseWithCreateWhenNotExists(TestContainer container)
            throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_create_when_not_exists.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 5);
    }

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

    @TestTemplate
    public void testFakeToTypesenseWithErrorWhenDataExists(TestContainer container)
            throws Exception {
        String initData = "{\"name\":\"Han\",\"age\":12}";
        typesenseClient.createCollection(sinkCollection);
        typesenseClient.insert(sinkCollection, Lists.newArrayList(initData));
        Assertions.assertEquals(typesenseClient.search(sinkCollection, null, 0).getFound(), 1);
        Container.ExecResult execResult =
                container.executeJob("/fake_to_typesense_with_error_when_data_exists.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
    }

    public List<String> genTestData(int recordNum) {
        ArrayList<String> testDataList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> doc = new HashMap<>();
        for (int i = 0; i < recordNum; i++) {
            try {
                doc.put("num_employees", RandomUtils.nextInt());
                doc.put("flag", RandomUtils.nextBoolean());
                doc.put("num", RandomUtils.nextLong());
                doc.put("company_name", "A" + RandomUtils.nextInt(1, 100));
                testDataList.add(objectMapper.writeValueAsString(doc));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return testDataList;
    }

    @TestTemplate
    public void testTypesenseSourceAndSink(TestContainer container) throws Exception {
        int recordNum = 100;
        List<String> testData = genTestData(recordNum);
        typesenseClient.createCollection(sourceCollection);
        typesenseClient.insert(sourceCollection, testData);
        Assertions.assertEquals(
                typesenseClient.search(sourceCollection, null, 0).getFound(), recordNum);
        Container.ExecResult execResult = container.executeJob("/typesense_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(
                typesenseClient.search(sinkCollection, null, 0).getFound(), recordNum);
    }

    @TestTemplate
    public void testTypesenseToTypesense(TestContainer container) throws Exception {
        String typesenseToTypesenseSource = "typesense_to_typesense_source";
        String typesenseToTypesenseSink = "typesense_to_typesense_sink";
        List<String> testData = new ArrayList<>();
        testData.add(
                "{\"c_row\":{\"c_array_int\":[12,45,96,8],\"c_int\":91,\"c_string\":\"String_412\"},\"company_name\":\"Company_9986\",\"company_name_list\":[\"Company_9986_Alias_1\",\"Company_9986_Alias_2\"],\"country\":\"Country_181\",\"id\":\"9986\",\"num_employees\":1914}");
        testData.add(
                "{\"c_row\":{\"c_array_int\":[60],\"c_int\":9,\"c_string\":\"String_371\"},\"company_name\":\"Company_9988\",\"company_name_list\":[\"Company_9988_Alias_1\",\"Company_9988_Alias_2\",\"Company_9988_Alias_3\"],\"country\":\"Country_86\",\"id\":\"9988\",\"num_employees\":7366}");
        typesenseClient.createCollection(typesenseToTypesenseSource);
        typesenseClient.insert(typesenseToTypesenseSource, testData);
        Assertions.assertEquals(
                typesenseClient.search(typesenseToTypesenseSource, null, 0).getFound(), 2);
        Container.ExecResult execResult = container.executeJob("/typesense_to_typesense.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(
                typesenseClient.search(typesenseToTypesenseSink, null, 0).getFound(), 2);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> sourceData = objectMapper.readValue(testData.get(0), Map.class);
        Map<String, Object> sinkData =
                typesenseClient
                        .search(typesenseToTypesenseSink, null, 0)
                        .getHits()
                        .get(0)
                        .getDocument();
        Assertions.assertNotEquals(sourceData.remove("id"), sinkData.remove("id"));
        Assertions.assertEquals(sourceData, sinkData);
    }

    @TestTemplate
    public void testTypesenseToTypesenseWithQuery(TestContainer container) throws Exception {
        String typesenseToTypesenseSource = "typesense_to_typesense_source_with_query";
        String typesenseToTypesenseSink = "typesense_to_typesense_sink_with_query";
        List<String> testData = new ArrayList<>();
        testData.add(
                "{\"c_row\":{\"c_array_int\":[12,45,96,8],\"c_int\":91,\"c_string\":\"String_412\"},\"company_name\":\"Company_9986\",\"company_name_list\":[\"Company_9986_Alias_1\",\"Company_9986_Alias_2\"],\"country\":\"Country_181\",\"id\":\"9986\",\"num_employees\":1914}");
        testData.add(
                "{\"c_row\":{\"c_array_int\":[60],\"c_int\":9,\"c_string\":\"String_371\"},\"company_name\":\"Company_9988\",\"company_name_list\":[\"Company_9988_Alias_1\",\"Company_9988_Alias_2\",\"Company_9988_Alias_3\"],\"country\":\"Country_86\",\"id\":\"9988\",\"num_employees\":7366}");
        testData.add(
                "{\"c_row\":{\"c_array_int\":[18,97],\"c_int\":32,\"c_string\":\"String_48\"},\"company_name\":\"Company_9880\",\"company_name_list\":[\"Company_9880_Alias_1\",\"Company_9880_Alias_2\",\"Company_9880_Alias_3\",\"Company_9880_Alias_4\"],\"country\":\"Country_159\",\"id\":\"9880\",\"num_employees\":141}");
        typesenseClient.createCollection(typesenseToTypesenseSource);
        typesenseClient.insert(typesenseToTypesenseSource, testData);
        Assertions.assertEquals(
                typesenseClient.search(typesenseToTypesenseSource, null, 0).getFound(), 3);
        Container.ExecResult execResult =
                container.executeJob("/typesense_to_typesense_with_query.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(
                typesenseClient.search(typesenseToTypesenseSink, null, 0).getFound(), 2);
    }

    @TestTemplate
    public void testCatalog(TestContainer container) {
        // Create table x 2
        TablePath tablePath = TablePath.of("tmp.tmp_table");
        TableIdentifier tableIdentifier = TableIdentifier.of("tmp_table", "tmp", "tmp_table");
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableIdentifier,
                        CatalogTable.of(
                                tableIdentifier,
                                TableSchema.builder()
                                        .column(
                                                new PhysicalColumn(
                                                        "id",
                                                        BasicType.LONG_TYPE,
                                                        null,
                                                        null,
                                                        false,
                                                        null,
                                                        ""))
                                        .build(),
                                new HashMap<>(),
                                new ArrayList<>(),
                                ""));
        Assertions.assertDoesNotThrow(() -> catalog.createTable(tablePath, catalogTable, false));
        Assertions.assertThrows(
                TableAlreadyExistException.class,
                () -> catalog.createTable(tablePath, catalogTable, false));
        Assertions.assertDoesNotThrow(() -> catalog.createTable(tablePath, catalogTable, true));

        // delete table
        Assertions.assertDoesNotThrow(() -> catalog.dropTable(tablePath, false));
        Assertions.assertThrows(
                TableNotExistException.class, () -> catalog.dropTable(tablePath, false));
        Assertions.assertDoesNotThrow(() -> catalog.dropTable(tablePath, true));
    }

    @AfterEach
    @Override
    public void tearDown() {
        typesenseServer.close();
        if (catalog != null) {
            catalog.close();
        }
    }
}
