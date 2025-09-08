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

package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class BsonToRowDataConvertersTest {
    private final BsonToRowDataConverters converterFactory = new BsonToRowDataConverters();

    @Test
    public void testConvertAnyNumberToDouble() {
        // It covered #6997
        BsonToRowDataConverters.BsonToRowDataConverter converter =
                converterFactory.createConverter(BasicType.DOUBLE_TYPE);

        Assertions.assertEquals(1.0d, converter.convert(new BsonInt32(1)));
        Assertions.assertEquals(1.0d, converter.convert(new BsonInt64(1L)));

        Assertions.assertEquals(4.0d, converter.convert(new BsonDouble(4.0d)));
        Assertions.assertEquals(4.4d, converter.convert(new BsonDouble(4.4d)));
    }

    @Test
    public void testConvertBsonNumberToLong() {
        // It covered #7567
        BsonToRowDataConverters.BsonToRowDataConverter converter =
                converterFactory.createConverter(BasicType.LONG_TYPE);

        Assertions.assertEquals(123456L, converter.convert(new BsonInt32(123456)));

        Assertions.assertEquals(
                (long) Integer.MAX_VALUE, converter.convert(new BsonInt64(Integer.MAX_VALUE)));

        Assertions.assertEquals(123456L, converter.convert(new BsonDouble(123456)));

        Assertions.assertThrowsExactly(
                MongodbConnectorException.class,
                () -> converter.convert(new BsonDouble(12345678901234567891234567890123456789.0d)));
    }

    @Test
    public void testConvertBsonNumberToInt() {
        // It covered #8042
        BsonToRowDataConverters.BsonToRowDataConverter converter =
                converterFactory.createConverter(BasicType.INT_TYPE);
        Assertions.assertEquals(123456, converter.convert(new BsonInt32(123456)));
        Assertions.assertEquals(
                Integer.MAX_VALUE, converter.convert(new BsonInt64(Integer.MAX_VALUE)));
        Assertions.assertEquals(123456, converter.convert(new BsonDouble(123456)));
        Assertions.assertThrowsExactly(
                MongodbConnectorException.class,
                () -> converter.convert(new BsonDouble(1234567890123456789.0d)));
    }

    @Test
    public void testConvertBsonDecimal128ToDecimal() {
        BsonToRowDataConverters.BsonToRowDataConverter converter =
                converterFactory.createConverter(new DecimalType(10, 2));
        Assertions.assertEquals(
                new BigDecimal("3.14"),
                converter.convert(new BsonDecimal128(Decimal128.parse("3.1415926"))));
    }

    @Test
    public void testConvertBsonToString() {
        BsonToRowDataConverters.BsonToRowDataConverter converter =
                converterFactory.createConverter(BasicType.STRING_TYPE);
        Assertions.assertEquals("123456", converter.convert(new BsonString("123456")));

        Assertions.assertEquals(
                "507f191e810c19729de860ea",
                converter.convert(new BsonObjectId(new ObjectId("507f191e810c19729de860ea"))));

        BsonDocument document =
                new BsonDocument()
                        .append("key", new BsonString("123456"))
                        .append("value", new BsonInt64(123456789L));
        Assertions.assertEquals(
                "{\"key\": \"123456\", \"value\": 123456789}", converter.convert(document));
    }

    @Test
    public void testConvertBsonToLocalDateTime() {
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        long epochMilli = now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        // localDataTime converter
        BsonToRowDataConverters.BsonToRowDataConverter localDataTimeConverter =
                converterFactory.createConverter(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        Assertions.assertEquals(now, localDataTimeConverter.convert(new BsonTimestamp(epochMilli)));
        Assertions.assertEquals(now, localDataTimeConverter.convert(new BsonDateTime(epochMilli)));

        // localDate converter
        BsonToRowDataConverters.BsonToRowDataConverter localDataConverter =
                converterFactory.createConverter(LocalTimeType.LOCAL_DATE_TYPE);
        Assertions.assertEquals(
                now.toLocalDate(), localDataConverter.convert(new BsonTimestamp(epochMilli)));
        Assertions.assertEquals(
                now.toLocalDate(), localDataConverter.convert(new BsonDateTime(epochMilli)));

        // localTime converter
        BsonToRowDataConverters.BsonToRowDataConverter localTimeConverter =
                converterFactory.createConverter(LocalTimeType.LOCAL_TIME_TYPE);
        Assertions.assertEquals(
                now.toLocalTime(), localTimeConverter.convert(new BsonTimestamp(epochMilli)));
        Assertions.assertEquals(
                now.toLocalTime(), localTimeConverter.convert(new BsonDateTime(epochMilli)));
    }
}
