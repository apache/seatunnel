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

package org.apache.seatunnel.transform.nlpmodel.embedding;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.Model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class EmbeddingBatchTransformTest {

    @Test
    void shouldDrainEmbeddingBatchWhenProcessBatchSizeReached() {
        EmbeddingTransform transform = new EmbeddingTransform(textConfig(2), textTable());
        transform.setModel(new FakeEmbeddingModel());

        transform.collect(textRow(1, "alpha"));
        Assertions.assertTrue(transform.drainOutput().isEmpty());

        transform.collect(textRow(2, "beta"));
        List<SeaTunnelRow> outputs = transform.drainOutput();

        Assertions.assertEquals(2, outputs.size());
        Assertions.assertTrue(outputs.get(0).getField(2) instanceof ByteBuffer);
        Assertions.assertTrue(outputs.get(1).getField(2) instanceof ByteBuffer);
    }

    @Test
    void shouldRestoreTextBatchFromCheckpoint() throws Exception {
        EmbeddingTransform transform = new EmbeddingTransform(textConfig(2), textTable());
        transform.setModel(new FakeEmbeddingModel());
        transform.collect(textRow(3, "gamma"));

        List<EmbeddingTransform.EmbeddingBatchState> states = transform.snapshotState(1L);
        Assertions.assertEquals(1, states.size());

        EmbeddingTransform restored = new EmbeddingTransform(textConfig(2), textTable());
        restored.setModel(new FakeEmbeddingModel());
        restored.restoreState(states);

        List<SeaTunnelRow> outputs = restored.flush();
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertTrue(outputs.get(0).getField(2) instanceof ByteBuffer);
    }

    @Test
    void shouldRestoreIncompleteBinaryChunksFromCheckpoint() throws Exception {
        EmbeddingTransform transform = new EmbeddingTransform(binaryConfig(), binaryTable());
        transform.setModel(new FakeEmbeddingModel());
        transform.collect(binaryRow(new byte[] {1, 2}, "image.png", 0L, false));

        List<EmbeddingTransform.EmbeddingBatchState> states = transform.snapshotState(1L);
        Assertions.assertEquals(1, states.size());

        EmbeddingTransform restored = new EmbeddingTransform(binaryConfig(), binaryTable());
        restored.setModel(new FakeEmbeddingModel());
        restored.restoreState(states);
        restored.collect(binaryRow(new byte[] {3, 4}, "image.png", 1L, true));

        List<SeaTunnelRow> outputs = restored.drainOutput();
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertTrue(outputs.get(0).getField(3) instanceof ByteBuffer);
    }

    private ReadonlyConfig textConfig(int batchSize) {
        Map<String, Object> vectorizationFields = new LinkedHashMap<>();
        vectorizationFields.put("text_vector", "text");

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("model_provider", "OPENAI");
        config.put("model", "text-embedding-3-small");
        config.put("api_key", "sk-test");
        config.put("process_batch_size", batchSize);
        config.put("single_vectorized_input_number", 16);
        config.put("vectorization_fields", vectorizationFields);
        return ReadonlyConfig.fromMap(config);
    }

    private ReadonlyConfig binaryConfig() {
        Map<String, Object> fieldConfig = new LinkedHashMap<>();
        fieldConfig.put("field", "data");
        fieldConfig.put("modality", "jpeg");
        fieldConfig.put("format", "binary");

        Map<String, Object> vectorizationFields = new LinkedHashMap<>();
        vectorizationFields.put("image_vector", fieldConfig);

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("model_provider", "DOUBAO");
        config.put("model", "doubao-embedding");
        config.put("api_key", "sk-test");
        config.put("process_batch_size", 1);
        config.put("single_vectorized_input_number", 16);
        config.put("vectorization_fields", vectorizationFields);
        return ReadonlyConfig.fromMap(config);
    }

    private CatalogTable textTable() {
        return CatalogTable.of(
                TableIdentifier.of("test", "db", "schema", "text_table"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, null, null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "text",
                                        BasicType.STRING_TYPE,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null))
                        .build(),
                new LinkedHashMap<>(),
                new ArrayList<>(),
                null);
    }

    private CatalogTable binaryTable() {
        return CatalogTable.of(
                TableIdentifier.of("test", "db", "schema", "binary_table"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "data",
                                        PrimitiveByteArrayType.INSTANCE,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "relative_path",
                                        BasicType.STRING_TYPE,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "part_index",
                                        BasicType.LONG_TYPE,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null))
                        .build(),
                new LinkedHashMap<>(),
                new ArrayList<>(),
                null);
    }

    private SeaTunnelRow textRow(int id, String text) {
        SeaTunnelRow row = new SeaTunnelRow(2);
        row.setField(0, id);
        row.setField(1, text);
        return row;
    }

    private SeaTunnelRow binaryRow(
            byte[] data, String relativePath, long partIndex, boolean complete) {
        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setField(0, data);
        row.setField(1, relativePath);
        row.setField(2, partIndex);
        MetadataUtil.setBinaryFormat(row);
        if (complete) {
            MetadataUtil.setBinaryRowComplete(row);
        }
        return row;
    }

    private static final class FakeEmbeddingModel implements Model {
        @Override
        public List<ByteBuffer> vectorization(Object[] fields) {
            List<ByteBuffer> outputs = new ArrayList<>(fields.length);
            for (int i = 0; i < fields.length; i++) {
                outputs.add(ByteBuffer.wrap(new byte[] {(byte) (i + 1), (byte) (i + 2)}));
            }
            return outputs;
        }

        @Override
        public Integer dimension() {
            return 2;
        }

        @Override
        public void close() throws IOException {}
    }
}
