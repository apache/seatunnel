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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Types;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OracleJdbcLobValueConvertersTest {

    @Test
    public void snapshotClobIsMaterializedWhenLobIsDisabled() throws Exception {
        ValueConverter converter = converter(clobColumn(true), Schema.OPTIONAL_STRING_SCHEMA);
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn(5L);
        when(clob.getSubString(1L, 5)).thenReturn("hello");

        Assertions.assertEquals("hello", converter.convert(clob));
    }

    @Test
    public void snapshotEmptyClobStaysEmpty() throws Exception {
        ValueConverter converter = converter(clobColumn(false), Schema.STRING_SCHEMA);
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn(0L);
        when(clob.getSubString(1L, 0)).thenReturn("");

        Assertions.assertEquals("", converter.convert(clob));
    }

    @Test
    public void snapshotBlobIsMaterializedWhenLobIsDisabled() throws Exception {
        ValueConverter converter = converter(blobColumn(false), Schema.OPTIONAL_BYTES_SCHEMA);
        Blob blob = mock(Blob.class);
        byte[] payload = new byte[] {1, 2, 3};
        when(blob.length()).thenReturn((long) payload.length);
        when(blob.getBytes(1L, payload.length)).thenReturn(payload);

        Assertions.assertArrayEquals(payload, bytes(converter.convert(blob)));
    }

    @Test
    public void streamingEmptyClobIsEmptyWhenLobMiningIsOn() {
        ValueConverter converter = converter(clobColumn(true), Schema.OPTIONAL_STRING_SCHEMA, true);

        Assertions.assertEquals("", converter.convert(OracleValueConverters.EMPTY_CLOB_FUNCTION));
        Assertions.assertNull(converter.convert(null));
    }

    @Test
    public void streamingEmptyClobStaysNullWhenLobMiningIsOff() {
        ValueConverter converter =
                converter(clobColumn(true), Schema.OPTIONAL_STRING_SCHEMA, false);

        Assertions.assertNull(converter.convert(OracleValueConverters.EMPTY_CLOB_FUNCTION));
    }

    @Test
    public void streamingEmptyBlobIsEmptyWhenLobMiningIsOn() {
        ValueConverter converter = converter(blobColumn(true), Schema.OPTIONAL_BYTES_SCHEMA, true);

        Assertions.assertArrayEquals(
                new byte[0], bytes(converter.convert(OracleValueConverters.EMPTY_BLOB_FUNCTION)));
        Assertions.assertNull(converter.convert(null));
    }

    @Test
    public void streamingUnavailableValueStaysThePlaceholder() throws Exception {
        ValueConverter converter = converter(clobColumn(true), Schema.OPTIONAL_STRING_SCHEMA);

        Assertions.assertEquals(
                "__debezium_unavailable_value",
                converter.convert(OracleValueConverters.UNAVAILABLE_VALUE));
        Assertions.assertEquals("keep-me", converter.convert("keep-me"));
    }

    private static byte[] bytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).asReadOnlyBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        throw new AssertionError("Unexpected binary value " + value);
    }

    private static ValueConverter converter(Column column, Schema fieldSchema) {
        return converter(column, fieldSchema, false);
    }

    private static ValueConverter converter(Column column, Schema fieldSchema, boolean lobEnabled) {
        OracleConnectorConfig config =
                new OracleConnectorConfig(
                        Configuration.create()
                                .with(OracleConnectorConfig.SERVER_NAME, "test_server")
                                .with(OracleConnectorConfig.HOSTNAME, "localhost")
                                .with(OracleConnectorConfig.USER, "test")
                                .with(OracleConnectorConfig.PASSWORD, "test")
                                .with(OracleConnectorConfig.LOB_ENABLED, lobEnabled)
                                .with(
                                        "unavailable.value.placeholder",
                                        "__debezium_unavailable_value")
                                .build());
        OracleJdbcLobValueConverters converters =
                new OracleJdbcLobValueConverters(config, mock(OracleConnection.class));
        Schema schema = SchemaBuilder.struct().field(column.name(), fieldSchema).build();
        Field field = schema.field(column.name());
        return converters.converter(column, field);
    }

    private static Column clobColumn(boolean optional) {
        return Column.editor()
                .name("VAL_CLOB")
                .type("CLOB")
                .jdbcType(Types.CLOB)
                .optional(optional)
                .position(1)
                .create();
    }

    private static Column blobColumn(boolean optional) {
        return Column.editor()
                .name("VAL_BLOB")
                .type("BLOB")
                .jdbcType(Types.BLOB)
                .optional(optional)
                .position(1)
                .create();
    }
}
