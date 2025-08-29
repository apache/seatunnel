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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.VectorUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class SQLVectorFunctionTest {

    private static final String TEST_NAME = "vector_test";
    private static final String[] FIELD_NAMES =
            new String[] {"id", "vector_field", "vector_field2"};
    private CatalogTable catalogTable;

    @BeforeEach
    void setUp() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        FIELD_NAMES,
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            VectorType.VECTOR_FLOAT_TYPE,
                            VectorType.VECTOR_FLOAT_TYPE
                        });

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            PhysicalColumn column =
                    PhysicalColumn.of(
                            rowType.getFieldName(i), rowType.getFieldType(i), 0, true, null, null);
            schemaBuilder.column(column);
        }

        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(TEST_NAME, TEST_NAME, null, TEST_NAME),
                        schemaBuilder.build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "Vector function test table");
    }

    @Test
    public void testVectorTruncate() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "SELECT id, VECTOR_REDUCE(vector_field, 3,'TRUNCATE') as truncated_vector FROM dual"));

        SQLTransform sqlTransform = new SQLTransform(config, catalogTable);
        TableSchema tableSchema = sqlTransform.transformTableSchema();

        // Create test data
        Float[] sourceVector = new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        ByteBuffer vectorBuffer = VectorUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, vectorBuffer, null});
        List<SeaTunnelRow> result = sqlTransform.transformRow(inputRow);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());

        SeaTunnelRow outputRow = result.get(0);
        Assertions.assertEquals(1, outputRow.getField(0));

        ByteBuffer resultVector = (ByteBuffer) outputRow.getField(1);
        Float[] resultArray = VectorUtils.toFloatArray(resultVector);
        Assertions.assertEquals(3, resultArray.length);
        Assertions.assertEquals(1.0f, resultArray[0], 0.001f);
        Assertions.assertEquals(2.0f, resultArray[1], 0.001f);
        Assertions.assertEquals(3.0f, resultArray[2], 0.001f);
    }

    @Test
    public void testVectorNormalize() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "SELECT id, VECTOR_NORMALIZE(vector_field) as normalized_vector FROM dual"));

        SQLTransform sqlTransform = new SQLTransform(config, catalogTable);

        // Create test data: [3, 4] normalized should be [0.6, 0.8]
        Float[] sourceVector = new Float[] {3.0f, 4.0f};
        ByteBuffer vectorBuffer = VectorUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, vectorBuffer, null});
        List<SeaTunnelRow> result = sqlTransform.transformRow(inputRow);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());

        SeaTunnelRow outputRow = result.get(0);
        Assertions.assertEquals(1, outputRow.getField(0));

        ByteBuffer resultVector = (ByteBuffer) outputRow.getField(1);
        Float[] resultArray = VectorUtils.toFloatArray(resultVector);
        Assertions.assertEquals(2, resultArray.length);
        Assertions.assertEquals(0.6f, resultArray[0], 0.001f);
        Assertions.assertEquals(0.8f, resultArray[1], 0.001f);
    }

    @Test
    public void testVectorReduce() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "SELECT id, VECTOR_REDUCE(vector_field, 3, 'TRUNCATE') as reduced_vector FROM dual"));

        SQLTransform sqlTransform = new SQLTransform(config, catalogTable);

        // Create test data
        Float[] sourceVector = new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        ByteBuffer vectorBuffer = VectorUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, vectorBuffer, null});
        List<SeaTunnelRow> result = sqlTransform.transformRow(inputRow);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());

        SeaTunnelRow outputRow = result.get(0);
        Assertions.assertEquals(1, outputRow.getField(0));

        ByteBuffer resultVector = (ByteBuffer) outputRow.getField(1);
        Float[] resultArray = VectorUtils.toFloatArray(resultVector);
        Assertions.assertEquals(3, resultArray.length);
        Assertions.assertEquals(1.0f, resultArray[0], 0.001f);
        Assertions.assertEquals(2.0f, resultArray[1], 0.001f);
        Assertions.assertEquals(3.0f, resultArray[2], 0.001f);
    }

    @Test
    public void testVectorRandomProjection() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "SELECT id, VECTOR_REDUCE(vector_field, 3,'RANDOM_PROJECTION') as projected_vector FROM dual"));

        SQLTransform sqlTransform = new SQLTransform(config, catalogTable);

        // Create test data
        Float[] sourceVector = new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        ByteBuffer vectorBuffer = VectorUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, vectorBuffer, null});
        List<SeaTunnelRow> result = sqlTransform.transformRow(inputRow);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());

        SeaTunnelRow outputRow = result.get(0);
        Assertions.assertEquals(1, outputRow.getField(0));

        ByteBuffer resultVector = (ByteBuffer) outputRow.getField(1);
        Float[] resultArray = VectorUtils.toFloatArray(resultVector);
        Assertions.assertEquals(3, resultArray.length);

        // Just verify that we got a result with the expected dimension
        for (Float value : resultArray) {
            Assertions.assertNotNull(value);
        }
    }

    @Test
    public void testVectorSparseProjection() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "SELECT id, VECTOR_REDUCE(vector_field, 3,'SPARSE_RANDOM_PROJECTION') as sparse_projected_vector FROM dual"));

        SQLTransform sqlTransform = new SQLTransform(config, catalogTable);

        // Create test data
        Float[] sourceVector = new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        ByteBuffer vectorBuffer = VectorUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, vectorBuffer, null});
        List<SeaTunnelRow> result = sqlTransform.transformRow(inputRow);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());

        SeaTunnelRow outputRow = result.get(0);
        Assertions.assertEquals(1, outputRow.getField(0));

        ByteBuffer resultVector = (ByteBuffer) outputRow.getField(1);
        Float[] resultArray = VectorUtils.toFloatArray(resultVector);
        Assertions.assertEquals(3, resultArray.length);

        // Just verify that we got a result with the expected dimension
        for (Float value : resultArray) {
            Assertions.assertNotNull(value);
        }
    }
}
