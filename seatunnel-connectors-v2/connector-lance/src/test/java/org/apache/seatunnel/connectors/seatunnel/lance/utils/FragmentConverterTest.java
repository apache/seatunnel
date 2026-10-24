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

package org.apache.seatunnel.connectors.seatunnel.lance.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.WriteParams;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class FragmentConverterTest {

    @Test
    void shouldMaterializeDatasetFieldsMissingFromRestoredRuntimeSchema() {
        SeaTunnelRowType restoredRowType =
                new SeaTunnelRowType(
                        new String[] {"c_string"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        Schema datasetSchema =
                new Schema(
                        Arrays.asList(
                                Field.nullable("c_string", new ArrowType.Utf8()),
                                Field.notNullable("c_bytes", new ArrowType.Binary())));

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root =
                        FragmentConverter.convertToVectorSchemaRoot(
                                new SeaTunnelRow(new Object[] {"restored"}),
                                restoredRowType,
                                datasetSchema,
                                allocator)) {
            Assertions.assertEquals(1, root.getRowCount());
            Assertions.assertEquals(
                    "restored",
                    new String(
                            ((VarCharVector) root.getVector("c_string")).get(0),
                            StandardCharsets.UTF_8));
            Assertions.assertTrue(root.getVector("c_bytes").isNull(0));
            Assertions.assertEquals(1, root.getVector("c_bytes").getValueCount());
        }
    }

    @Test
    void shouldPackMultipleRowsIntoOneVectorSchemaRoot() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_string"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        Schema schema = new Schema(Arrays.asList(Field.nullable("c_string", new ArrowType.Utf8())));

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root =
                        FragmentConverter.convertToVectorSchemaRoot(
                                Arrays.asList(
                                        new SeaTunnelRow(new Object[] {"row-0"}),
                                        new SeaTunnelRow(new Object[] {"row-1"}),
                                        new SeaTunnelRow(new Object[] {"row-2"})),
                                rowType,
                                schema,
                                allocator)) {
            Assertions.assertEquals(3, root.getRowCount());
            VarCharVector vector = (VarCharVector) root.getVector("c_string");
            Assertions.assertEquals("row-0", new String(vector.get(0), StandardCharsets.UTF_8));
            Assertions.assertEquals("row-1", new String(vector.get(1), StandardCharsets.UTF_8));
            Assertions.assertEquals("row-2", new String(vector.get(2), StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldPackMultipleArrayRowsIntoOneVectorSchemaRoot() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_floats"},
                        new SeaTunnelDataType<?>[] {ArrayType.FLOAT_ARRAY_TYPE});
        Field arrayField =
                new Field(
                        "c_floats",
                        FieldType.nullable(new ArrowType.List()),
                        Collections.singletonList(
                                Field.nullable(
                                        "item",
                                        new ArrowType.FloatingPoint(
                                                FloatingPointPrecision.SINGLE))));
        Schema schema = new Schema(Collections.singletonList(arrayField));
        List<Float> fiveValues = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f);
        List<Float> oneValue = Collections.singletonList(6.0f);
        List<Float> empty = Collections.emptyList();

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root =
                        FragmentConverter.convertToVectorSchemaRoot(
                                Arrays.asList(
                                        new SeaTunnelRow(new Object[] {fiveValues}),
                                        new SeaTunnelRow(new Object[] {oneValue}),
                                        new SeaTunnelRow(new Object[] {empty}),
                                        new SeaTunnelRow(new Object[] {null})),
                                rowType,
                                schema,
                                allocator)) {
            Assertions.assertEquals(4, root.getRowCount());
            ListVector vector = (ListVector) root.getVector("c_floats");
            Assertions.assertEquals(fiveValues, vector.getObject(0));
            Assertions.assertEquals(oneValue, vector.getObject(1));
            Assertions.assertFalse(vector.isNull(2));
            Assertions.assertEquals(empty, vector.getObject(2));
            Assertions.assertTrue(vector.isNull(3));
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldWriteOneFragmentPerBatch() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_string"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        Schema schema = new Schema(Arrays.asList(Field.nullable("c_string", new ArrowType.Utf8())));
        List<SeaTunnelRow> rows =
                Arrays.asList(
                        new SeaTunnelRow(new Object[] {"row-0"}),
                        new SeaTunnelRow(new Object[] {"row-1"}),
                        new SeaTunnelRow(new Object[] {"row-2"}));

        Path tempDir = Files.createTempDirectory("lance-batch-fragment");
        try (RootAllocator allocator = new RootAllocator()) {
            String datasetPath = tempDir.resolve("batch.lance").toString();
            createEmptyDataset(allocator, datasetPath, schema);
            List<FragmentMetadata> fragments =
                    FragmentConverter.reconvert(rows, rowType, schema, allocator, datasetPath);
            Assertions.assertEquals(1, fragments.size());
            Assertions.assertEquals(3, fragments.get(0).getNumRows());
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void shouldRejectMapColumnsWhenBuildingSchema() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "attributes"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE)
                        });

        LanceConnectorException exception =
                Assertions.assertThrows(
                        LanceConnectorException.class,
                        () ->
                                SchemaUtils.convertSchema(
                                        new SeaTunnelRow(new Object[] {1, null}), rowType));

        Assertions.assertEquals(
                "ErrorCode:[LANCE-05], ErrorDescription:[Lance does not support Map columns] - "
                        + "Field 'attributes' has type MAP. Serialize the Map to a string or "
                        + "flatten it into columns before writing to Lance.",
                exception.getMessage());
    }

    private static void createEmptyDataset(
            RootAllocator allocator, String datasetPath, Schema schema) {
        try (Dataset ignored =
                Dataset.create(
                        allocator,
                        datasetPath,
                        schema,
                        new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build())) {
            // An empty dataset gives Fragment.create a place to write data files.
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        Files.walkFileTree(
                root,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                            throws IOException {
                        if (exc != null) {
                            throw exc;
                        }
                        Files.deleteIfExists(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }
}
