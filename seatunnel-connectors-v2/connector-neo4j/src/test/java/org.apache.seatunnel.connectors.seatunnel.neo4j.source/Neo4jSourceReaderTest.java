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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import static org.apache.seatunnel.api.table.type.ArrayType.STRING_ARRAY_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.BytesValue;
import org.neo4j.driver.internal.value.DateValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.LocalDateTimeValue;
import org.neo4j.driver.internal.value.LocalTimeValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;

class Neo4jSourceReaderTest {
    @Test
    void convertType() {
        assertEquals("test", Neo4jSourceReader.convertType(BasicType.STRING_TYPE, new StringValue("test")));
        assertEquals(true, Neo4jSourceReader.convertType(BasicType.BOOLEAN_TYPE, BooleanValue.TRUE));
        assertEquals(1L, Neo4jSourceReader.convertType(BasicType.LONG_TYPE, new IntegerValue(1L)));
        assertEquals(1.5, Neo4jSourceReader.convertType(BasicType.DOUBLE_TYPE, new FloatValue(1.5)));
        assertNull(Neo4jSourceReader.convertType(BasicType.VOID_TYPE, NullValue.NULL));
        assertEquals((byte) 1, ((byte[]) Neo4jSourceReader.convertType(PrimitiveByteArrayType.INSTANCE, new BytesValue(new byte[]{(byte) 1})))[0]);
        assertEquals(LocalDate.MIN, Neo4jSourceReader.convertType(LocalTimeType.LOCAL_DATE_TYPE, new DateValue(LocalDate.MIN)));
        assertEquals(LocalTime.MIN, Neo4jSourceReader.convertType(LocalTimeType.LOCAL_TIME_TYPE, new LocalTimeValue(LocalTime.MIN)));
        assertEquals(LocalDateTime.MIN, Neo4jSourceReader.convertType(LocalTimeType.LOCAL_DATE_TIME_TYPE, new LocalDateTimeValue(LocalDateTime.MIN)));
        assertEquals(Collections.singletonMap("1", false),
            Neo4jSourceReader.convertType(new MapType<>(BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE), new MapValue(Collections.singletonMap("1", BooleanValue.FALSE))));
        assertArrayEquals(new Object[]{"foo", "bar"},
            (Object[]) Neo4jSourceReader.convertType(STRING_ARRAY_TYPE, new ListValue(new StringValue("foo"), new StringValue("bar"))));
        assertEquals(1, Neo4jSourceReader.convertType(BasicType.INT_TYPE, new IntegerValue(1)));
        assertEquals(1.1F, Neo4jSourceReader.convertType(BasicType.FLOAT_TYPE, new FloatValue(1.1F)));

        assertThrows(Neo4jConnectorException.class, () -> Neo4jSourceReader.convertType(BasicType.SHORT_TYPE, new IntegerValue(256)));
        assertThrows(LossyCoercion.class, () -> Neo4jSourceReader.convertType(BasicType.INT_TYPE, new IntegerValue(Integer.MAX_VALUE + 1L)));
        assertThrows(Neo4jConnectorException.class, () -> Neo4jSourceReader.convertType(new MapType<>(BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE), new MapValue(Collections.singletonMap("1", BooleanValue.FALSE))));
    }
}
