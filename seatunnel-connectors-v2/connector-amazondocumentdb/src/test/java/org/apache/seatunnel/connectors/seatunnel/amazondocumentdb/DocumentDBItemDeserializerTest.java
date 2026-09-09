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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.serialize.DocumentDBItemDeserializer;

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
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;

public class DocumentDBItemDeserializerTest {

    @Test
    public void testDeserializeBsonDocument() {
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"city"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "id", "name", "active", "score", "amount", "created", "payload", "tags",
                            "labels", "address", "missing"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(10, 2),
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            ArrayType.of(BasicType.STRING_TYPE),
                            new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE),
                            addressType,
                            BasicType.STRING_TYPE
                        });

        ObjectId objectId = new ObjectId("507f191e810c19729de860ea");
        long epochMillis = 1704067200000L;
        BsonDocument document =
                new BsonDocument()
                        .append("id", new BsonObjectId(objectId))
                        .append("name", new BsonString("documentdb-user"))
                        .append("active", BsonBoolean.TRUE)
                        .append("score", new BsonDouble(98.5))
                        .append("amount", new BsonDecimal128(Decimal128.parse("123.456")))
                        .append("created", new BsonDateTime(epochMillis))
                        .append("payload", new BsonBinary(new byte[] {1, 2, 3}))
                        .append(
                                "tags",
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("alpha"), new BsonString("beta"))))
                        .append("labels", new BsonDocument("priority", new BsonInt32(2)))
                        .append("address", new BsonDocument("city", new BsonString("Seattle")))
                        .append("missing", new BsonNull());

        SeaTunnelRow row = new DocumentDBItemDeserializer(rowType).deserialize(document);

        Assertions.assertEquals(objectId.toHexString(), row.getField(0));
        Assertions.assertEquals("documentdb-user", row.getField(1));
        Assertions.assertEquals(true, row.getField(2));
        Assertions.assertEquals(98.5d, row.getField(3));
        Assertions.assertEquals(new BigDecimal("123.46"), row.getField(4));
        Assertions.assertEquals(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault()),
                row.getField(5));
        Assertions.assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) row.getField(6));
        Assertions.assertArrayEquals(new String[] {"alpha", "beta"}, (Object[]) row.getField(7));
        Assertions.assertEquals(2, ((Map<?, ?>) row.getField(8)).get("priority"));
        Assertions.assertEquals("Seattle", ((SeaTunnelRow) row.getField(9)).getField(0));
        Assertions.assertNull(row.getField(10));
    }

    @Test
    public void testRejectsIncompatibleBsonType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        BsonDocument document = new BsonDocument("value", new BsonString("not-an-int"));

        Assertions.assertThrows(
                RuntimeException.class,
                () -> new DocumentDBItemDeserializer(rowType).deserialize(document));
    }

    @Test
    public void testNumericConversions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"intValue", "longValue"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.LONG_TYPE});
        BsonDocument document =
                new BsonDocument()
                        .append("intValue", new BsonInt64(7))
                        .append("longValue", new BsonInt32(9));

        SeaTunnelRow row = new DocumentDBItemDeserializer(rowType).deserialize(document);

        Assertions.assertEquals(7, row.getField(0));
        Assertions.assertEquals(9L, row.getField(1));
    }

    @Test
    public void testPreservesNumericConversionFailureCause() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.BYTE_TYPE});
        BsonDocument document = new BsonDocument("value", new BsonInt32(128));

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> new DocumentDBItemDeserializer(rowType).deserialize(document));

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertTrue(exception.getCause().getMessage().contains("out of range"));
    }

    @Test
    public void testRejectsDecimalPrecisionOverflow() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {new DecimalType(4, 2)});
        BsonDocument document =
                new BsonDocument("value", new BsonDecimal128(Decimal128.parse("123.45")));

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> new DocumentDBItemDeserializer(rowType).deserialize(document));

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertTrue(exception.getCause().getMessage().contains("exceeds"));
    }
}
