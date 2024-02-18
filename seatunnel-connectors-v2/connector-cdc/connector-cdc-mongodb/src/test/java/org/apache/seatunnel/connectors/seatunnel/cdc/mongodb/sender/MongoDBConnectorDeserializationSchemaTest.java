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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.sender;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.OperationType;

import javax.annotation.Nonnull;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.VOID_TYPE;
import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.CLUSTER_TIME_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COLL_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DB_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FALSE_FALSE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FULL_DOCUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SOURCE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.TS_MS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.currentBsonTimestamp;

public class MongoDBConnectorDeserializationSchemaTest {

    private static final String HOSTS = "localhost";
    private static final String DB = "db";
    private static final String COLL = "coll";
    private static final MongoNamespace NS = new MongoNamespace(DB, COLL);
    private static final String IDENTIFIER = "MongoDB-CDC";

    @Test
    public void testDeserialize() {
        final String[] FIELD_NAMES =
                new String[] {
                    "_id",
                    "c_boolean",
                    "c_double",
                    "c_int",
                    "c_long",
                    "c_bytes",
                    "c_string",
                    "c_date",
                    "c_timestamp",
                    "c_decimal",
                    "c_array",
                    "c_map",
                    "c_row"
                };
        final SeaTunnelDataType<?>[] FIELD_TYPES =
                new SeaTunnelDataType[] {
                    INT_TYPE,
                    BOOLEAN_TYPE,
                    DOUBLE_TYPE,
                    INT_TYPE,
                    LONG_TYPE,
                    PrimitiveByteArrayType.INSTANCE,
                    STRING_TYPE,
                    LocalTimeType.LOCAL_DATE_TYPE,
                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    new DecimalType(30, 10),
                    ArrayType.INT_ARRAY_TYPE,
                    new MapType(STRING_TYPE, STRING_TYPE),
                    new SeaTunnelRowType(
                            new String[] {"c_int", "c_string"},
                            new SeaTunnelDataType[] {INT_TYPE, STRING_TYPE})
                };
        final MultipleRowType multipleRowType =
                new MultipleRowType(
                        new String[] {NS.getFullName()},
                        new SeaTunnelRowType[] {new SeaTunnelRowType(FIELD_NAMES, FIELD_TYPES)});

        final MongoDBConnectorDeserializationSchema deser =
                new MongoDBConnectorDeserializationSchema(multipleRowType, multipleRowType);
        final SimpleCollector collector = new SimpleCollector();

        Map<String, String> mapField = new HashMap<>();
        mapField.put("k0", "v0");
        Object[] rowFields =
                new Object[] {
                    1,
                    true,
                    3.14,
                    128,
                    128L,
                    "test".getBytes(),
                    "test",
                    LocalDate.now(),
                    LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS),
                    BigDecimal.valueOf(999999, 10),
                    new Integer[] {0, 1, 2},
                    mapField,
                    new SeaTunnelRow(new Object[] {0, "test"})
                };

        SeaTunnelRow row = new SeaTunnelRow(rowFields);
        row.setRowKind(RowKind.INSERT);
        /**
         * Why set tableId to null? Because {@link
         * MongoDBConnectorDeserializationSchema#extractTableId} is not implemented.
         */
        row.setTableId(null);

        BsonDocument cdcDoc = new BsonDocument();
        BsonDocument idDoc = new BsonDocument();
        idDoc.put(ID_FIELD, new BsonInt32(1));

        BsonDocument rowDoc = new BsonDocument();
        rowDoc.put(ID_FIELD, new BsonInt32((int) rowFields[0]));
        rowDoc.put(FIELD_NAMES[1], new BsonBoolean((boolean) rowFields[1]));
        rowDoc.put(FIELD_NAMES[2], new BsonDouble((double) rowFields[2]));
        rowDoc.put(FIELD_NAMES[3], new BsonInt32((int) rowFields[3]));
        rowDoc.put(FIELD_NAMES[4], new BsonInt64((long) rowFields[4]));
        rowDoc.put(FIELD_NAMES[5], new BsonBinary((byte[]) rowFields[5]));
        rowDoc.put(FIELD_NAMES[6], new BsonString((String) rowFields[6]));
        rowDoc.put(
                FIELD_NAMES[7],
                new BsonDateTime(
                        Timestamp.valueOf(((LocalDate) rowFields[7]).atStartOfDay()).getTime()));
        rowDoc.put(
                FIELD_NAMES[8],
                new BsonTimestamp(
                        (int)
                                TimeUnit.MILLISECONDS.toSeconds(
                                        Timestamp.valueOf((LocalDateTime) rowFields[8]).getTime()),
                        0));
        rowDoc.put(FIELD_NAMES[9], new BsonDecimal128(new Decimal128((BigDecimal) rowFields[9])));

        List<BsonInt32> bsonInt32List =
                Arrays.stream(((Integer[]) rowFields[10]))
                        .mapToInt(x -> x)
                        .mapToObj(BsonInt32::new)
                        .collect(Collectors.toList());
        rowDoc.put(FIELD_NAMES[10], new BsonArray(bsonInt32List));

        Map<String, String> mapF = (Map<String, String>) rowFields[11];
        BsonDocument mapDoc = new BsonDocument();
        mapF.forEach(
                (String k, String v) -> {
                    mapDoc.put(k, new BsonString(v));
                });
        rowDoc.put(FIELD_NAMES[11], mapDoc);

        SeaTunnelRow rowF = (SeaTunnelRow) rowFields[12];
        BsonDocument rowFDoc = new BsonDocument();
        rowFDoc.put("c_int", new BsonInt32((int) rowF.getField(0)));
        rowFDoc.put("c_string", new BsonString((String) rowF.getField(1)));
        rowDoc.put(FIELD_NAMES[12], rowFDoc);

        BsonDocument nsDoc = new BsonDocument();
        nsDoc.put(DB_FIELD, new BsonString(DB));
        nsDoc.put(COLL_FIELD, new BsonString(COLL));

        cdcDoc.put(ID_FIELD, idDoc);
        cdcDoc.put(OPERATION_TYPE, new BsonString(OperationType.INSERT.getValue()));
        cdcDoc.put(DOCUMENT_KEY, idDoc);
        cdcDoc.put(FULL_DOCUMENT, rowDoc);
        cdcDoc.put(NS_FIELD, nsDoc);

        BsonDocument valueDocument = normalizeChangeStreamDocument(cdcDoc);
        Map<String, String> partitionMap =
                MongodbRecordUtils.createPartitionMap(
                        HOSTS, NS.getDatabaseName(), NS.getCollectionName());
        Map<String, String> offsetMap = MongodbRecordUtils.createSourceOffsetMap(idDoc, false);
        SourceRecord record =
                MongodbRecordUtils.buildSourceRecord(
                        partitionMap, offsetMap, NS.getFullName(), idDoc, valueDocument);

        deser.deserialize(record, collector);
        Assertions.assertEquals(1, collector.list.size());

        SeaTunnelRow actual = collector.list.get(0);

        Assertions.assertEquals(row.getField(0), actual.getField(0));
        Assertions.assertEquals(row.getField(1), actual.getField(1));
        Assertions.assertEquals(row.getField(2), actual.getField(2));
        Assertions.assertEquals(row.getField(3), actual.getField(3));
        Assertions.assertEquals(row.getField(4), actual.getField(4));
        Assertions.assertArrayEquals((byte[]) row.getField(5), (byte[]) actual.getField(5));
        Assertions.assertEquals(row.getField(6), actual.getField(6));
        Assertions.assertEquals(row.getField(7), actual.getField(7));
        Assertions.assertEquals(row.getField(8), actual.getField(8));
        Assertions.assertEquals(row.getField(9), actual.getField(9));
        Assertions.assertArrayEquals((Integer[]) row.getField(10), (Integer[]) actual.getField(10));
        Assertions.assertEquals(row.getField(11), actual.getField(11));
        Assertions.assertEquals(row.getField(12), actual.getField(12));
    }

    @Test
    public void testConvertToNull() {
        final String[] FIELD_NAMES = new String[] {"_id", "c_mock"};
        final SeaTunnelDataType<?>[] FIELD_TYPES = new SeaTunnelDataType[] {INT_TYPE, VOID_TYPE};
        final MultipleRowType multipleRowType =
                new MultipleRowType(
                        new String[] {NS.getFullName()},
                        new SeaTunnelRowType[] {new SeaTunnelRowType(FIELD_NAMES, FIELD_TYPES)});

        final MongoDBConnectorDeserializationSchema deser =
                new MongoDBConnectorDeserializationSchema(multipleRowType, multipleRowType);
        final SimpleCollector collector = new SimpleCollector();

        BsonValue mockVal = new BsonNull();
        SourceRecord record = mockRecord("c_mock", mockVal);
        deser.deserialize(record, collector);

        SeaTunnelRow actual = collector.list.get(0);
        Assertions.assertEquals(1, actual.getField(0));
        Assertions.assertEquals(null, actual.getField(1));
    }

    @Test
    public void testConvertToBooleanError() {
        BsonValue mockVal = new BsonInt32(0);
        testConvertError(BOOLEAN_TYPE, mockVal);
    }

    @Test
    public void testConvertToDoubleError() {
        BsonValue mockVal = new BsonInt32(0);
        testConvertError(DOUBLE_TYPE, mockVal);
    }

    @Test
    public void testConvertToIntError() {
        BsonValue mockVal = new BsonBoolean(false);
        testConvertError(INT_TYPE, mockVal);
    }

    @Test
    public void testConvertToLongError() {
        BsonValue mockVal = new BsonBoolean(false);
        testConvertError(LONG_TYPE, mockVal);
    }

    @Test
    public void testConvertToBinaryError() {
        BsonValue mockVal = new BsonBoolean(false);
        testConvertError(PrimitiveByteArrayType.INSTANCE, mockVal);
    }

    @Test
    public void testConvertBsonBooleanToString() {
        final String[] FIELD_NAMES = new String[] {"_id", "c_mock"};
        final SeaTunnelDataType<?>[] FIELD_TYPES = new SeaTunnelDataType[] {INT_TYPE, STRING_TYPE};
        final MultipleRowType multipleRowType =
                new MultipleRowType(
                        new String[] {NS.getFullName()},
                        new SeaTunnelRowType[] {new SeaTunnelRowType(FIELD_NAMES, FIELD_TYPES)});

        final MongoDBConnectorDeserializationSchema deser =
                new MongoDBConnectorDeserializationSchema(multipleRowType, multipleRowType);
        final SimpleCollector collector = new SimpleCollector();

        BsonValue mockVal = new BsonBoolean(false);
        SourceRecord record = mockRecord("c_mock", mockVal);
        deser.deserialize(record, collector);

        SeaTunnelRow actual = collector.list.get(0);
        Assertions.assertEquals(1, actual.getField(0));
        Assertions.assertEquals("{\"_value\": false}", actual.getField(1));
    }

    @Test
    public void testConvertToDateError() {
        BsonValue mockVal = new BsonBoolean(false);
        MongodbConnectorException expected =
                new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Unable to convert to LocalDateTime from unexpected value '"
                                + mockVal
                                + "' of type "
                                + mockVal.getBsonType());
        testConvertError(
                LocalTimeType.LOCAL_DATE_TYPE, mockVal, expected, MongodbConnectorException.class);
    }

    @Test
    public void testConvertToTimeStampError() {
        BsonValue mockVal = new BsonBoolean(false);
        MongodbConnectorException expected =
                new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Unable to convert to LocalDateTime from unexpected value '"
                                + mockVal
                                + "' of type "
                                + mockVal.getBsonType());
        testConvertError(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                mockVal,
                expected,
                MongodbConnectorException.class);
    }

    @Test
    public void testConvertToDecimalError() {
        BsonValue mockVal = new BsonBoolean(false);
        testConvertError(new DecimalType(30, 10), mockVal);
    }

    @Test
    public void testConvertToArrayError() {
        BsonArray mockVal = new BsonArray();
        BsonBoolean mockEle = new BsonBoolean(false);
        mockVal.add(mockEle);

        /** Test convert unmatched type element of Array */
        SeaTunnelRuntimeException expected =
                CommonError.convertToSeaTunnelTypeError(
                        IDENTIFIER, mockEle.getBsonType().name(), "c_mock[*]");
        testConvertError(
                ArrayType.INT_ARRAY_TYPE, mockVal, expected, SeaTunnelRuntimeException.class);

        /** Test convert non-Array to Array */
        MongodbConnectorException illegalArgumentExpected =
                new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Unable to convert to arrayType from unexpected value '"
                                + mockEle
                                + "' of type "
                                + mockEle.getBsonType());
        testConvertError(
                ArrayType.INT_ARRAY_TYPE,
                mockEle,
                illegalArgumentExpected,
                MongodbConnectorException.class);
    }

    @Test
    public void testConvertToMapError() {
        MapType type = new MapType<>(STRING_TYPE, INT_TYPE);
        BsonBinary mockVal = new BsonBinary("test".getBytes());

        /** Test convert non-Map to Map */
        MongodbConnectorException illegalArgumentExpected =
                new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Unable to convert to rowType from unexpected value '"
                                + mockVal
                                + "' of type "
                                + mockVal.getBsonType());

        testConvertError(type, mockVal, illegalArgumentExpected, MongodbConnectorException.class);

        BsonDocument mockMap = new BsonDocument();
        BsonBoolean mockV = new BsonBoolean(false);
        mockMap.put("k0", mockV);

        /** Test convert unmatched Value Type of Map */
        SeaTunnelRuntimeException expected =
                CommonError.convertToSeaTunnelTypeError(
                        IDENTIFIER, mockV.getBsonType().name(), "c_mock[VAL]");
        testConvertError(type, mockMap, expected, SeaTunnelRuntimeException.class);
    }

    @Test
    public void testConvertToRowError() {
        SeaTunnelRowType type =
                new SeaTunnelRowType(new String[] {"c_0"}, new SeaTunnelDataType[] {INT_TYPE});
        BsonDocument rowDoc = new BsonDocument();
        BsonValue f = new BsonBoolean(false);
        rowDoc.put("c_0", f);

        SeaTunnelRuntimeException expected =
                CommonError.convertToSeaTunnelTypeError(
                        IDENTIFIER, f.getBsonType().name(), "c_mock.c_0");
        testConvertError(type, rowDoc, expected, SeaTunnelRuntimeException.class);
    }

    public void testConvertError(SeaTunnelDataType<?> mockFieldType, BsonValue mockVal) {
        SeaTunnelRuntimeException expected =
                CommonError.convertToSeaTunnelTypeError(
                        IDENTIFIER, mockVal.getBsonType().name(), "c_mock");
        testConvertError(mockFieldType, mockVal, expected, SeaTunnelRuntimeException.class);
    }

    public void testConvertError(
            SeaTunnelDataType<?> mockFieldType,
            BsonValue mockVal,
            Throwable expected,
            Class<? extends Throwable> expectedType) {
        final String[] FIELD_NAMES = new String[] {"_id", "c_mock"};
        final SeaTunnelDataType<?>[] FIELD_TYPES =
                new SeaTunnelDataType[] {INT_TYPE, mockFieldType};
        final MultipleRowType multipleRowType =
                new MultipleRowType(
                        new String[] {NS.getFullName()},
                        new SeaTunnelRowType[] {new SeaTunnelRowType(FIELD_NAMES, FIELD_TYPES)});

        final MongoDBConnectorDeserializationSchema deser =
                new MongoDBConnectorDeserializationSchema(multipleRowType, multipleRowType);
        final SimpleCollector collector = new SimpleCollector();

        String mockField = FIELD_NAMES[1];
        SourceRecord record = mockRecord(mockField, mockVal);

        Throwable actual =
                Assertions.assertThrows(expectedType, () -> deser.deserialize(record, collector));
        Assertions.assertEquals(expected.getMessage(), actual.getMessage());
    }

    private SourceRecord mockRecord(String field, BsonValue mockVal) {
        BsonDocument cdcDoc = new BsonDocument();
        BsonDocument idDoc = new BsonDocument();
        idDoc.put(ID_FIELD, new BsonInt32(1));
        BsonDocument rowDoc = new BsonDocument();
        rowDoc.put(ID_FIELD, new BsonInt32(1));
        rowDoc.put(field, mockVal);

        BsonDocument nsDoc = new BsonDocument();
        nsDoc.put(DB_FIELD, new BsonString(DB));
        nsDoc.put(COLL_FIELD, new BsonString(COLL));

        cdcDoc.put(ID_FIELD, idDoc);
        cdcDoc.put(OPERATION_TYPE, new BsonString(OperationType.INSERT.getValue()));
        cdcDoc.put(DOCUMENT_KEY, idDoc);
        cdcDoc.put(FULL_DOCUMENT, rowDoc);
        cdcDoc.put(NS_FIELD, nsDoc);

        BsonDocument valueDocument = normalizeChangeStreamDocument(cdcDoc);
        Map<String, String> partitionMap =
                MongodbRecordUtils.createPartitionMap(
                        HOSTS, NS.getDatabaseName(), NS.getCollectionName());
        Map<String, String> offsetMap = MongodbRecordUtils.createSourceOffsetMap(idDoc, false);
        SourceRecord record =
                MongodbRecordUtils.buildSourceRecord(
                        partitionMap, offsetMap, NS.getFullName(), idDoc, valueDocument);
        return record;
    }

    private BsonDocument normalizeChangeStreamDocument(@Nonnull BsonDocument changeStreamDocument) {
        // _id: primary key of change document.
        BsonDocument normalizedDocument = normalizeKeyDocument(changeStreamDocument);
        changeStreamDocument.put(ID_FIELD, normalizedDocument);

        // ts_ms: It indicates the time at which the reader processed the event.
        changeStreamDocument.put(TS_MS_FIELD, new BsonInt64(System.currentTimeMillis()));

        // source
        BsonDocument source = new BsonDocument();
        source.put(SNAPSHOT_FIELD, new BsonString(FALSE_FALSE));

        if (!changeStreamDocument.containsKey(CLUSTER_TIME_FIELD)) {
            changeStreamDocument.put(CLUSTER_TIME_FIELD, currentBsonTimestamp());
        }

        BsonTimestamp clusterTime = changeStreamDocument.getTimestamp(CLUSTER_TIME_FIELD);
        Instant clusterInstant = Instant.ofEpochSecond(clusterTime.getTime());
        source.put(TS_MS_FIELD, new BsonInt64(clusterInstant.toEpochMilli()));
        changeStreamDocument.put(SOURCE_FIELD, source);

        return changeStreamDocument;
    }

    private BsonDocument normalizeKeyDocument(@Nonnull BsonDocument changeStreamDocument) {
        BsonDocument documentKey = changeStreamDocument.getDocument(DOCUMENT_KEY);
        BsonDocument primaryKey = new BsonDocument(ID_FIELD, documentKey.get(ID_FIELD));
        return new BsonDocument(ID_FIELD, primaryKey);
    }

    private static class SimpleCollector implements Collector<SeaTunnelRow> {

        private List<SeaTunnelRow> list = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            list.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }
}
