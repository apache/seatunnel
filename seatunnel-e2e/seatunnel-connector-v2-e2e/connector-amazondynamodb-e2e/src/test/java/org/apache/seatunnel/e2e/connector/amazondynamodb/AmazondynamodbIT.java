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

package org.apache.seatunnel.e2e.connector.amazondynamodb;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
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
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.math.BigDecimal;
import java.net.ConnectException;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class AmazondynamodbIT extends TestSuiteBase implements TestResource {
    private static final String AMAZONDYNAMODB_DOCKER_IMAGE = "amazon/dynamodb-local:1.21.0";
    private static final String AMAZONDYNAMODB_CONTAINER_HOST = "dynamodb-host";
    private static final int AMAZONDYNAMODB_CONTAINER_PORT = 8000;
    private static final String AMAZONDYNAMODB_JOB_CONFIG = "/amazondynamodbIT_source_to_sink.conf";
    private static final String SINK_TABLE = "sink_table";
    private static final String SOURCE_TABLE = "source_table";
    private static final String PARTITION_KEY = "id";

    private GenericContainer<?> dynamoDB;
    protected DynamoDbClient dynamoDbClient;

    @TestTemplate
    public void testAmazondynamodb(TestContainer container) throws Exception {
        assertHasData(SOURCE_TABLE);
        Container.ExecResult execResult = container.executeJob(AMAZONDYNAMODB_JOB_CONFIG);
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SOURCE_TABLE);
        assertHasData(SINK_TABLE);
        compareResult();
        clearSinkTable();
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        dynamoDB =
                new GenericContainer<>(AMAZONDYNAMODB_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(AMAZONDYNAMODB_CONTAINER_HOST)
                        .withExposedPorts(AMAZONDYNAMODB_CONTAINER_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                AMAZONDYNAMODB_DOCKER_IMAGE)));
        dynamoDB.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s",
                                AMAZONDYNAMODB_CONTAINER_PORT, AMAZONDYNAMODB_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(dynamoDB)).join();
        log.info("dynamodb container started");
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(this::initializeDynamodbClient);
        batchInsertData();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (dynamoDB != null) {
            dynamoDB.close();
        }
    }

    private void initializeDynamodbClient() throws ConnectException {
        dynamoDbClient =
                DynamoDbClient.builder()
                        .endpointOverride(
                                URI.create(
                                        "http://"
                                                + dynamoDB.getHost()
                                                + ":"
                                                + AMAZONDYNAMODB_CONTAINER_PORT))
                        // The region is meaningless for local DynamoDb but required for client
                        // builder validation
                        .region(Region.US_EAST_1)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create("dummy-key", "dummy-secret")))
                        .build();

        createTable(dynamoDbClient, SOURCE_TABLE);
        createTable(dynamoDbClient, SINK_TABLE);
    }

    private void batchInsertData() {
        dynamoDbClient.putItem(
                PutItemRequest.builder().tableName(SOURCE_TABLE).item(randomRow()).build());
    }

    private void clearSinkTable() {
        dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(SINK_TABLE).build());
        createTable(dynamoDbClient, SINK_TABLE);
    }

    private void assertHasData(String tableName) {
        ScanResponse scan =
                dynamoDbClient.scan(
                        ScanRequest.builder().tableName(tableName).consistentRead(true).build());
        Assertions.assertTrue(
                !scan.items().isEmpty(), String.format("table %s is empty.", tableName));
    }

    private void compareResult() {
        Map<String, AttributeValue> sourceAttributeValueMap =
                dynamoDbClient
                        .scan(ScanRequest.builder().tableName(SOURCE_TABLE).build())
                        .items()
                        .get(0);
        Map<String, AttributeValue> sinkAttributeValueMap =
                dynamoDbClient
                        .scan(ScanRequest.builder().tableName(SINK_TABLE).build())
                        .items()
                        .get(0);
        sourceAttributeValueMap
                .keySet()
                .forEach(
                        key -> {
                            AttributeValue sourceAttributeValue = sourceAttributeValueMap.get(key);
                            AttributeValue sinkAttributeValue = sinkAttributeValueMap.get(key);
                            Assertions.assertEquals(sourceAttributeValue, sinkAttributeValue);
                        });
    }

    private Map<String, AttributeValue> randomRow() {
        SeaTunnelRowType seatunnelRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "id",
                            "c_map",
                            "c_array",
                            "c_string",
                            "c_boolean",
                            "c_tinyint",
                            "c_smallint",
                            "c_int",
                            "c_bigint",
                            "c_float",
                            "c_double",
                            "c_decimal",
                            "c_bytes",
                            "c_date",
                            "c_timestamp"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            new MapType(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
                            ArrayType.BYTE_ARRAY_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(2, 1),
                            PrimitiveByteArrayType.INSTANCE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            "1",
                            Collections.singletonMap("key", Short.parseShort("1")),
                            new Byte[] {Byte.parseByte("1")},
                            "string",
                            Boolean.FALSE,
                            Byte.parseByte("1"),
                            Short.parseShort("1"),
                            Integer.parseInt("1"),
                            Long.parseLong("1"),
                            Float.parseFloat("1.1"),
                            Double.parseDouble("1.1"),
                            BigDecimal.valueOf(11, 1),
                            "test".getBytes(),
                            LocalDate.now(),
                            LocalDateTime.now()
                        });

        Map<String, AttributeValue> data = new HashMap<>(seatunnelRowType.getTotalFields());
        for (int index = 0; index < seatunnelRowType.getTotalFields(); index++) {
            data.put(
                    seatunnelRowType.getFieldName(index),
                    convertItem(
                            row.getField(index),
                            seatunnelRowType.getFieldType(index),
                            convertType(seatunnelRowType.getFieldType(index))));
        }
        return data;
    }

    private static void createTable(DynamoDbClient ddb, String tableName) {
        DynamoDbWaiter dbWaiter = ddb.waiter();
        CreateTableRequest request =
                CreateTableRequest.builder()
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName(PARTITION_KEY)
                                        .attributeType(ScalarAttributeType.S)
                                        .build())
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName(PARTITION_KEY)
                                        .keyType(KeyType.HASH)
                                        .build())
                        .provisionedThroughput(
                                ProvisionedThroughput.builder()
                                        .readCapacityUnits(10L)
                                        .writeCapacityUnits(10L)
                                        .build())
                        .tableName(tableName)
                        .build();

        try {
            ddb.createTable(request);
            DescribeTableRequest tableRequest =
                    DescribeTableRequest.builder().tableName(tableName).build();

            // Wait until the Amazon DynamoDB table is created.
            WaiterResponse<DescribeTableResponse> waiterResponse =
                    dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse
                    .matched()
                    .response()
                    .ifPresent(
                            describeTableResponse -> {
                                log.info(describeTableResponse.toString());
                            });

        } catch (DynamoDbException e) {
            log.error(e.getMessage());
        }
    }

    private AttributeValue convertItem(
            Object value,
            SeaTunnelDataType seaTunnelDataType,
            AttributeValue.Type measurementsType) {
        if (value == null) {
            return AttributeValue.builder().nul(true).build();
        }
        switch (measurementsType) {
            case N:
                return AttributeValue.builder()
                        .n(Integer.toString(((Number) value).intValue()))
                        .build();
            case S:
                return AttributeValue.builder().s(String.valueOf(value)).build();
            case BOOL:
                return AttributeValue.builder().bool((Boolean) value).build();
            case B:
                return AttributeValue.builder()
                        .b(SdkBytes.fromByteArrayUnsafe((byte[]) value))
                        .build();
            case SS:
                return AttributeValue.builder().ss((Collection<String>) value).build();
            case NS:
                return AttributeValue.builder()
                        .ns(
                                ((Collection<Number>) value)
                                        .stream()
                                                .map(Object::toString)
                                                .collect(Collectors.toList()))
                        .build();
            case BS:
                return AttributeValue.builder()
                        .bs(
                                ((Collection<Number>) value)
                                        .stream()
                                                .map(
                                                        number ->
                                                                SdkBytes.fromByteArray(
                                                                        (byte[]) value))
                                                .collect(Collectors.toList()))
                        .build();
            case M:
                MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelDataType;
                Map<String, Object> map = (Map) value;
                Map<String, AttributeValue> resultMap = new HashMap<>(map.size());
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String mapKeyName = entry.getKey();
                    resultMap.put(
                            mapKeyName,
                            convertItem(
                                    entry.getValue(),
                                    mapType.getValueType(),
                                    convertType(mapType.getValueType())));
                }
                return AttributeValue.builder().m(resultMap).build();
            case L:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelDataType;
                SeaTunnelDataType<?> elementType = arrayType.getElementType();
                Object[] l = (Object[]) value;
                return AttributeValue.builder()
                        .l(
                                Stream.of(l)
                                        .map(
                                                o ->
                                                        convertItem(
                                                                o,
                                                                elementType,
                                                                convertType(elementType)))
                                        .collect(Collectors.toList()))
                        .build();
            case NUL:
                return AttributeValue.builder().nul(true).build();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported dataType: " + measurementsType);
        }
    }

    private AttributeValue.Type convertType(SeaTunnelDataType seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case INT:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return AttributeValue.Type.N;
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return AttributeValue.Type.S;
            case BOOLEAN:
                return AttributeValue.Type.BOOL;
            case NULL:
                return AttributeValue.Type.NUL;
            case BYTES:
                return AttributeValue.Type.B;
            case MAP:
                return AttributeValue.Type.M;
            case ARRAY:
                return AttributeValue.Type.L;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported dataType: " + seaTunnelDataType);
        }
    }
}
