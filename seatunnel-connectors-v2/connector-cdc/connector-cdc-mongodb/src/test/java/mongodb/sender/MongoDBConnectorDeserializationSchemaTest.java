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

package mongodb.sender;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.sender.MongoDBConnectorDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COLL_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DB_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FULL_DOCUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE_INSERT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_TRUE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SOURCE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.TS_MS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createSourceOffsetMap;

public class MongoDBConnectorDeserializationSchemaTest {

    private static TableSchema tableSchema;
    private static CatalogTable catalogTable;

    @BeforeAll
    public static void setUp() {
        tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("int", BasicType.INT_TYPE, 1L, true, null, ""))
                        .column(PhysicalColumn.of("long", BasicType.LONG_TYPE, 1L, true, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "double", BasicType.DOUBLE_TYPE, 1L, true, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "decimal", new DecimalType(10, 2), 1L, true, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "string", BasicType.STRING_TYPE, 200L, true, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "date",
                                        LocalTimeType.LOCAL_DATE_TYPE,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "time",
                                        LocalTimeType.LOCAL_TIME_TYPE,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "timestamp",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null))
                        .build();
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
    }

    @Test
    public void extractTableId() {
        MongoDBConnectorDeserializationSchema schema =
                new MongoDBConnectorDeserializationSchema(Collections.singletonList(catalogTable));

        // Build SourceRecord
        Map<String, String> partitionMap =
                MongodbRecordUtils.createPartitionMap("localhost:27017", "inventory", "products");

        BsonDocument valueDocument =
                new BsonDocument()
                        .append(
                                ID_FIELD,
                                new BsonDocument(ID_FIELD, new BsonInt64(10000000000001L)))
                        .append(OPERATION_TYPE, new BsonString(OPERATION_TYPE_INSERT))
                        .append(
                                NS_FIELD,
                                new BsonDocument(DB_FIELD, new BsonString("inventory"))
                                        .append(COLL_FIELD, new BsonString("products")))
                        .append(
                                DOCUMENT_KEY,
                                new BsonDocument(ID_FIELD, new BsonInt64(10000000000001L)))
                        .append(FULL_DOCUMENT, new BsonDocument())
                        .append(TS_MS_FIELD, new BsonInt64(System.currentTimeMillis()))
                        .append(
                                SOURCE_FIELD,
                                new BsonDocument(SNAPSHOT_FIELD, new BsonString(SNAPSHOT_TRUE))
                                        .append(TS_MS_FIELD, new BsonInt64(0L)));
        BsonDocument keyDocument = new BsonDocument(ID_FIELD, valueDocument.get(ID_FIELD));
        SourceRecord sourceRecord =
                MongodbRecordUtils.buildSourceRecord(
                        partitionMap,
                        createSourceOffsetMap(keyDocument.getDocument(ID_FIELD), true),
                        "inventory.products",
                        keyDocument,
                        valueDocument);
        Object tableId = schema.extractTableIdForTest(sourceRecord);
        Assertions.assertEquals("inventory.products", tableId);
    }

    @Test
    public void testBsonConvert() {
        MongoDBConnectorDeserializationSchema schema =
                new MongoDBConnectorDeserializationSchema(Collections.singletonList(catalogTable));
        // check int
        Assertions.assertEquals(
                123456, schema.convertToObject(getDataType("int"), new BsonInt32(123456)));
        Assertions.assertEquals(
                Integer.MAX_VALUE,
                schema.convertToObject(getDataType("int"), new BsonInt64(Integer.MAX_VALUE)));
        Assertions.assertEquals(
                123456, schema.convertToObject(getDataType("int"), new BsonDouble(123456)));
        Assertions.assertThrowsExactly(
                MongodbConnectorException.class,
                () ->
                        schema.convertToObject(
                                getDataType("int"), new BsonDouble(1234567890123456789.0d)));
        Assertions.assertThrowsExactly(
                MongodbConnectorException.class,
                () -> schema.convertToObject(getDataType("int"), new BsonInt64(Long.MIN_VALUE)));
        // check long
        Assertions.assertEquals(
                123456L, schema.convertToObject(getDataType("long"), new BsonInt32(123456)));
        Assertions.assertEquals(
                (long) Integer.MAX_VALUE,
                schema.convertToObject(getDataType("long"), new BsonInt64(Integer.MAX_VALUE)));
        Assertions.assertEquals(
                123456L, schema.convertToObject(getDataType("long"), new BsonDouble(123456)));
        Assertions.assertThrowsExactly(
                MongodbConnectorException.class,
                () ->
                        schema.convertToObject(
                                getDataType("long"),
                                new BsonDouble(12345678901234567891234567890123456789.0d)));

        // check double
        Assertions.assertEquals(
                1.0d, schema.convertToObject(getDataType("double"), new BsonInt32(1)));
        Assertions.assertEquals(
                1.0d, schema.convertToObject(getDataType("double"), new BsonInt64(1)));
        Assertions.assertEquals(
                4.4d, schema.convertToObject(getDataType("double"), new BsonDouble(4.4)));
        // check decimal
        Assertions.assertEquals(
                new BigDecimal("3.14"),
                schema.convertToObject(
                        getDataType("decimal"), new BsonDecimal128(Decimal128.parse("3.1415926"))));
        // check string
        Assertions.assertEquals(
                "123456", schema.convertToObject(getDataType("string"), new BsonString("123456")));
        Assertions.assertEquals(
                "507f191e810c19729de860ea",
                schema.convertToObject(
                        getDataType("string"),
                        new BsonObjectId(new ObjectId("507f191e810c19729de860ea"))));
        BsonDocument document =
                new BsonDocument()
                        .append("key", new BsonString("123456"))
                        .append("value", new BsonInt64(123456789L));
        Assertions.assertEquals(
                "{\"key\": \"123456\", \"value\": 123456789}",
                schema.convertToObject(getDataType("string"), document));

        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        long epochMilli = now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        // check localDate
        Assertions.assertEquals(
                now.toLocalDate(),
                schema.convertToObject(getDataType("date"), new BsonDateTime(epochMilli)));
        Assertions.assertEquals(
                now.toLocalDate(),
                schema.convertToObject(getDataType("date"), new BsonDateTime(epochMilli)));
        // check localTime
        Assertions.assertEquals(
                now.toLocalTime(),
                schema.convertToObject(getDataType("time"), new BsonDateTime(epochMilli)));
        Assertions.assertEquals(
                now.toLocalTime(),
                schema.convertToObject(getDataType("time"), new BsonDateTime(epochMilli)));
        // check localDateTime
        Assertions.assertEquals(
                now,
                schema.convertToObject(getDataType("timestamp"), new BsonDateTime(epochMilli)));
        Assertions.assertEquals(
                now,
                schema.convertToObject(getDataType("timestamp"), new BsonDateTime(epochMilli)));
    }

    private SeaTunnelDataType<?> getDataType(String fieldName) {
        String[] fieldNames = tableSchema.getFieldNames();
        return IntStream.range(0, fieldNames.length)
                .mapToObj(
                        i -> {
                            if (fieldName.equals(fieldNames[i])) {
                                return tableSchema.getColumns().get(i).getDataType();
                            }
                            return null;
                        })
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("not found field"));
    }
}
