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

package org.apache.seatunnel.connectors.seatunnel.cassandra.util;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TypeConvertUtilTest {

    @Test
    void testBuildSeaTunnelRowKeepsNullTimestamp() {
        Row row = mock(Row.class);
        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        ColumnDefinition columnDefinition = mock(ColumnDefinition.class);

        when(row.size()).thenReturn(1);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(columnDefinitions.get(0)).thenReturn(columnDefinition);
        when(columnDefinition.getType()).thenReturn(DataTypes.TIMESTAMP);
        when(row.getInstant(0)).thenReturn(null);

        SeaTunnelRow seaTunnelRow = TypeConvertUtil.buildSeaTunnelRow(row);

        Assertions.assertNull(seaTunnelRow.getField(0));
    }

    @Test
    void nullCellsBecomeNullFieldsForEveryType() {
        RawType text = RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR);
        RawType integer = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT);
        List<RawType> types =
                new ArrayList<>(
                        Arrays.asList(
                                new RawType.RawList(text),
                                new RawType.RawList(integer),
                                new RawType.RawSet(text),
                                new RawType.RawMap(text, integer)));
        for (int code :
                new int[] {
                    ProtocolConstants.DataType.ASCII,
                    ProtocolConstants.DataType.VARCHAR,
                    ProtocolConstants.DataType.VARINT,
                    ProtocolConstants.DataType.TIMEUUID,
                    ProtocolConstants.DataType.UUID,
                    ProtocolConstants.DataType.INET,
                    ProtocolConstants.DataType.TINYINT,
                    ProtocolConstants.DataType.SMALLINT,
                    ProtocolConstants.DataType.INT,
                    ProtocolConstants.DataType.BIGINT,
                    ProtocolConstants.DataType.FLOAT,
                    ProtocolConstants.DataType.DOUBLE,
                    ProtocolConstants.DataType.DECIMAL,
                    ProtocolConstants.DataType.BOOLEAN,
                    ProtocolConstants.DataType.TIME,
                    ProtocolConstants.DataType.DATE,
                    ProtocolConstants.DataType.TIMESTAMP,
                    ProtocolConstants.DataType.BLOB
                }) {
            types.add(RawType.PRIMITIVES.get(code));
        }

        for (RawType type : types) {
            SeaTunnelRow seaTunnelRow = TypeConvertUtil.buildSeaTunnelRow(driverRow(type, null));

            Assertions.assertNull(seaTunnelRow.getField(0), "protocol type " + type.id);
        }
    }

    @Test
    void storedZeroAndFalseAreStillRead() {
        RawType integer = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT);
        RawType bool = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BOOLEAN);

        Assertions.assertEquals(
                0,
                TypeConvertUtil.buildSeaTunnelRow(
                                driverRow(
                                        integer, TypeCodecs.INT.encode(0, ProtocolVersion.DEFAULT)))
                        .getField(0));
        Assertions.assertEquals(
                false,
                TypeConvertUtil.buildSeaTunnelRow(
                                driverRow(
                                        bool,
                                        TypeCodecs.BOOLEAN.encode(false, ProtocolVersion.DEFAULT)))
                        .getField(0));
    }

    @Test
    void nullValuesAreBoundAsNullForEveryType() {
        List<DataType> types =
                Arrays.asList(
                        DataTypes.ASCII,
                        DataTypes.TEXT,
                        DataTypes.VARINT,
                        DataTypes.TIMEUUID,
                        DataTypes.UUID,
                        DataTypes.INET,
                        DataTypes.TINYINT,
                        DataTypes.SMALLINT,
                        DataTypes.INT,
                        DataTypes.BIGINT,
                        DataTypes.COUNTER,
                        DataTypes.FLOAT,
                        DataTypes.DOUBLE,
                        DataTypes.DECIMAL,
                        DataTypes.BOOLEAN,
                        DataTypes.TIME,
                        DataTypes.DATE,
                        DataTypes.TIMESTAMP,
                        DataTypes.BLOB,
                        DataTypes.listOf(DataTypes.TEXT),
                        DataTypes.setOf(DataTypes.TEXT),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));

        for (DataType type : types) {
            BoundStatement statement = mock(BoundStatement.class, RETURNS_SELF);

            Assertions.assertSame(
                    statement,
                    TypeConvertUtil.reconvertAndInject(statement, 3, type, null),
                    type.toString());

            verify(statement).setToNull(3);
            verifyNoMoreInteractions(statement);
        }
    }

    @Test
    void nonNullValuesAreStillBound() {
        BoundStatement statement = mock(BoundStatement.class, RETURNS_SELF);

        TypeConvertUtil.reconvertAndInject(statement, 0, DataTypes.INT, 0);

        verify(statement).setInt(0, 0);
        verifyNoMoreInteractions(statement);
    }

    /** Builds a real driver row, whose getters decode a null value the way a CQL NULL is read. */
    private static Row driverRow(RawType type, ByteBuffer value) {
        ColumnDefinition definition =
                new DefaultColumnDefinition(
                        new ColumnSpec("test", "t", "c", 0, type), AttachmentPoint.NONE);
        return new DefaultRow(
                DefaultColumnDefinitions.valueOf(Collections.singletonList(definition)),
                Collections.singletonList(value),
                AttachmentPoint.NONE);
    }
}
