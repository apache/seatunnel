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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.VectorUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.yashandb.vector.Vector;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class YashanDbJdbcRowConverterTest {

    private final YashanDbJdbcRowConverter converter = new YashanDbJdbcRowConverter();

    @Test
    public void testReadFloatVectorFromYashanDbVectorObject() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(Vector.ofFloat32Values(new float[] {1.25f, 2.5f}));

        SeaTunnelRow row =
                converter.toInternal(resultSet, createSchema(VectorType.VECTOR_FLOAT_TYPE));

        ByteBuffer buffer = ((ByteBuffer) row.getField(0)).duplicate();
        Assertions.assertEquals(1.25f, buffer.getFloat());
        Assertions.assertEquals(2.5f, buffer.getFloat());
    }

    @Test
    public void testWriteFloatVectorUsesYashanDbVectorApi() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        ByteBuffer buffer = ByteBuffer.allocate(2 * Float.BYTES);
        buffer.putFloat(1.25f);
        buffer.putFloat(2.5f);
        buffer.flip();

        converter.toExternal(
                createSchema(VectorType.VECTOR_FLOAT_TYPE),
                new SeaTunnelRow(new Object[] {buffer}),
                statement);

        ArgumentCaptor<Vector> vectorCaptor = ArgumentCaptor.forClass(Vector.class);
        verify(statement).setObject(eq(1), vectorCaptor.capture());
        Assertions.assertArrayEquals(
                new float[] {1.25f, 2.5f}, vectorCaptor.getValue().toFloatArray());
        verify(statement, never()).setNull(1, Types.NULL);
    }

    @Test
    public void testWriteFloat16VectorDecodesTwoByteElements() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(
                createSchema(VectorType.VECTOR_FLOAT16_TYPE),
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Short[] {0x3c00, (short) 0xc000})
                        }),
                statement);

        ArgumentCaptor<Vector> vectorCaptor = ArgumentCaptor.forClass(Vector.class);
        verify(statement).setObject(eq(1), vectorCaptor.capture());
        Assertions.assertArrayEquals(
                new float[] {1.0f, -2.0f}, vectorCaptor.getValue().toFloatArray());
    }

    @Test
    public void testWriteBFloat16VectorDecodesTwoByteElements() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(
                createSchema(VectorType.VECTOR_BFLOAT16_TYPE),
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Short[] {0x3f80, (short) 0xc000})
                        }),
                statement);

        ArgumentCaptor<Vector> vectorCaptor = ArgumentCaptor.forClass(Vector.class);
        verify(statement).setObject(eq(1), vectorCaptor.capture());
        Assertions.assertArrayEquals(
                new float[] {1.0f, -2.0f}, vectorCaptor.getValue().toFloatArray());
    }

    @Test
    public void testWriteNullVectorUsesExplicitJdbcType() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(
                createSchema(VectorType.VECTOR_FLOAT_TYPE),
                new SeaTunnelRow(new Object[] {null}),
                statement);

        verify(statement).setNull(1, Types.VARCHAR);
        verify(statement, never()).setObject(eq(1), any());
    }

    @Test
    public void testWriteFloat32ArrayUsesYashanDbVectorApi() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(
                createSchema(ArrayType.INT_ARRAY_TYPE),
                new SeaTunnelRow(new Object[] {new Integer[] {1, 2}}),
                statement);

        ArgumentCaptor<Vector> vectorCaptor = ArgumentCaptor.forClass(Vector.class);
        verify(statement).setObject(eq(1), vectorCaptor.capture());
        Assertions.assertArrayEquals(
                new float[] {1.0f, 2.0f}, vectorCaptor.getValue().toFloatArray());
    }

    @Test
    public void testWriteFloat64ArrayUsesYashanDbVectorApi() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(
                createSchema(ArrayType.DOUBLE_ARRAY_TYPE),
                new SeaTunnelRow(new Object[] {new Double[] {1.25d, 2.5d}}),
                statement);

        ArgumentCaptor<Vector> vectorCaptor = ArgumentCaptor.forClass(Vector.class);
        verify(statement).setObject(eq(1), vectorCaptor.capture());
        Assertions.assertArrayEquals(
                new double[] {1.25d, 2.5d}, vectorCaptor.getValue().toDoubleArray());
    }

    private static TableSchema createSchema(SeaTunnelDataType<?> dataType) {
        return TableSchema.builder()
                .columns(
                        Collections.singletonList(
                                PhysicalColumn.builder()
                                        .name("vector_col")
                                        .dataType(dataType)
                                        .build()))
                .build();
    }
}
