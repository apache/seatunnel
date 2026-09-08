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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.DriverBuilder;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.internal.InternalRecord;
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

import static org.apache.seatunnel.api.table.type.ArrayType.STRING_ARRAY_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Neo4jSourceReaderTest {

    @Test
    void mapsRowsUsingTableSpecificSchemaAndTableId() {
        SeaTunnelRowType peopleRowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        Neo4jSourceTableConfig peopleConfig =
                new Neo4jSourceTableConfig("people query", peopleRowType, "people");
        InternalRecord peopleRecord =
                new InternalRecord(
                        Collections.singletonList("name"), new Value[] {new StringValue("Alice")});

        SeaTunnelRow peopleRow = Neo4jSourceReader.convertRecord(peopleRecord, peopleConfig);

        assertEquals("Alice", peopleRow.getField(0));
        assertEquals("people", peopleRow.getTableId());

        SeaTunnelRowType companiesRowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        Neo4jSourceTableConfig companiesConfig =
                new Neo4jSourceTableConfig("companies query", companiesRowType, "companies");
        InternalRecord companiesRecord =
                new InternalRecord(
                        Collections.singletonList("id"), new Value[] {new IntegerValue(7)});

        SeaTunnelRow companiesRow =
                Neo4jSourceReader.convertRecord(companiesRecord, companiesConfig);

        assertEquals(7, companiesRow.getField(0));
        assertEquals("companies", companiesRow.getTableId());

        Neo4jSourceTableConfig singleTableConfig =
                new Neo4jSourceTableConfig("single query", peopleRowType, null);
        SeaTunnelRow singleTableRow =
                Neo4jSourceReader.convertRecord(peopleRecord, singleTableConfig);
        assertEquals("", singleTableRow.getTableId());
    }

    @Test
    void includesTableIdAndSignalsCompletionWhenMultiTableReadFails() throws Exception {
        SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
        Session session = mock(Session.class);
        ServiceUnavailableException failure = new ServiceUnavailableException("connection refused");
        when(session.readTransaction(any(TransactionWork.class))).thenThrow(failure);
        Neo4jSourceTableConfig tableConfig =
                new Neo4jSourceTableConfig("MATCH (n) RETURN n", rowType(), "people");
        Neo4jSourceReader reader = reader(context, session, tableConfig);
        Collector<SeaTunnelRow> collector = mock(Collector.class);

        reader.open();
        Neo4jConnectorException thrown =
                assertThrows(
                        Neo4jConnectorException.class, () -> reader.internalPollNext(collector));

        assertTrue(thrown.getMessage().contains("people"));
        assertSame(failure, thrown.getCause());
        verify(context).signalNoMoreElement();
    }

    @Test
    void keepsOriginalFailureForSingleTableRead() throws Exception {
        SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
        Session session = mock(Session.class);
        ServiceUnavailableException failure = new ServiceUnavailableException("connection refused");
        when(session.readTransaction(any(TransactionWork.class))).thenThrow(failure);
        Neo4jSourceTableConfig tableConfig =
                new Neo4jSourceTableConfig("MATCH (n) RETURN n", rowType(), null);
        Neo4jSourceReader reader = reader(context, session, tableConfig);
        Collector<SeaTunnelRow> collector = mock(Collector.class);

        reader.open();
        ServiceUnavailableException thrown =
                assertThrows(
                        ServiceUnavailableException.class,
                        () -> reader.internalPollNext(collector));

        assertSame(failure, thrown);
        verify(context).signalNoMoreElement();
    }

    @Test
    void convertType() {
        assertEquals(
                "test",
                Neo4jSourceReader.convertType(BasicType.STRING_TYPE, new StringValue("test")));
        assertEquals(
                true, Neo4jSourceReader.convertType(BasicType.BOOLEAN_TYPE, BooleanValue.TRUE));
        assertEquals(1L, Neo4jSourceReader.convertType(BasicType.LONG_TYPE, new IntegerValue(1L)));
        assertEquals(
                1.5, Neo4jSourceReader.convertType(BasicType.DOUBLE_TYPE, new FloatValue(1.5)));
        assertNull(Neo4jSourceReader.convertType(BasicType.VOID_TYPE, NullValue.NULL));
        assertEquals(
                (byte) 1,
                ((byte[])
                                Neo4jSourceReader.convertType(
                                        PrimitiveByteArrayType.INSTANCE,
                                        new BytesValue(new byte[] {(byte) 1})))
                        [0]);
        assertEquals(
                LocalDate.MIN,
                Neo4jSourceReader.convertType(
                        LocalTimeType.LOCAL_DATE_TYPE, new DateValue(LocalDate.MIN)));
        assertEquals(
                LocalTime.MIN,
                Neo4jSourceReader.convertType(
                        LocalTimeType.LOCAL_TIME_TYPE, new LocalTimeValue(LocalTime.MIN)));
        assertEquals(
                LocalDateTime.MIN,
                Neo4jSourceReader.convertType(
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        new LocalDateTimeValue(LocalDateTime.MIN)));
        assertEquals(
                Collections.singletonMap("1", false),
                Neo4jSourceReader.convertType(
                        new MapType<>(BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE),
                        new MapValue(Collections.singletonMap("1", BooleanValue.FALSE))));
        assertArrayEquals(
                new Object[] {"foo", "bar"},
                (Object[])
                        Neo4jSourceReader.convertType(
                                STRING_ARRAY_TYPE,
                                new ListValue(new StringValue("foo"), new StringValue("bar"))));
        assertEquals(1, Neo4jSourceReader.convertType(BasicType.INT_TYPE, new IntegerValue(1)));
        assertEquals(
                1.1F, Neo4jSourceReader.convertType(BasicType.FLOAT_TYPE, new FloatValue(1.1F)));

        assertThrows(
                Neo4jConnectorException.class,
                () -> Neo4jSourceReader.convertType(BasicType.SHORT_TYPE, new IntegerValue(256)));
        assertThrows(
                LossyCoercion.class,
                () ->
                        Neo4jSourceReader.convertType(
                                BasicType.INT_TYPE, new IntegerValue(Integer.MAX_VALUE + 1L)));
        assertThrows(
                Neo4jConnectorException.class,
                () ->
                        Neo4jSourceReader.convertType(
                                new MapType<>(BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE),
                                new MapValue(Collections.singletonMap("1", BooleanValue.FALSE))));
    }

    private Neo4jSourceReader reader(
            SingleSplitReaderContext context, Session session, Neo4jSourceTableConfig tableConfig) {
        Driver driver = mock(Driver.class);
        DriverBuilder driverBuilder = mock(DriverBuilder.class);
        Neo4jSourceQueryInfo queryInfo = mock(Neo4jSourceQueryInfo.class);
        when(driverBuilder.build()).thenReturn(driver);
        when(driverBuilder.getDatabase()).thenReturn("neo4j");
        when(driver.session(any(SessionConfig.class))).thenReturn(session);
        when(queryInfo.getDriverBuilder()).thenReturn(driverBuilder);
        return new Neo4jSourceReader(context, queryInfo, Collections.singletonList(tableConfig));
    }

    private SeaTunnelRowType rowType() {
        return new SeaTunnelRowType(
                new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
    }
}
