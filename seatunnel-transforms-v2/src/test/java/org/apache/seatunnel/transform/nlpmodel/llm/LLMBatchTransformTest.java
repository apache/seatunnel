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

package org.apache.seatunnel.transform.nlpmodel.llm;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.Model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class LLMBatchTransformTest {

    @Test
    void shouldDrainBatchWhenProcessBatchSizeReached() {
        LLMTransform transform = new LLMTransform(batchConfig(2), defaultTable());
        transform.setModel(new FakeLLMModel());

        transform.collect(row(1, "alpha"));
        Assertions.assertTrue(transform.drainOutput().isEmpty());

        transform.collect(row(2, "beta"));
        List<SeaTunnelRow> outputs = transform.drainOutput();

        Assertions.assertEquals(2, outputs.size());
        Assertions.assertEquals("result-1", outputs.get(0).getField(2));
        Assertions.assertEquals("result-2", outputs.get(1).getField(2));
    }

    @Test
    void shouldRestoreBufferedRowsFromCheckpoint() throws Exception {
        LLMTransform transform = new LLMTransform(batchConfig(2), defaultTable());
        transform.setModel(new FakeLLMModel());
        transform.collect(row(3, "gamma"));

        List<LLMTransform.LLMBatchState> states = transform.snapshotState(1L);
        Assertions.assertEquals(1, states.size());

        LLMTransform restored = new LLMTransform(batchConfig(2), defaultTable());
        restored.setModel(new FakeLLMModel());
        restored.restoreState(states);

        List<SeaTunnelRow> outputs = restored.flush();
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertEquals("result-3", outputs.get(0).getField(2));
    }

    private ReadonlyConfig batchConfig(int batchSize) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("model_provider", "OPENAI");
        config.put("model", "gpt-4o-mini");
        config.put("api_key", "sk-test");
        config.put("prompt", "test");
        config.put("process_batch_size", batchSize);
        config.put("output_column_name", "llm_output");
        config.put("output_data_type", "STRING");
        return ReadonlyConfig.fromMap(config);
    }

    private CatalogTable defaultTable() {
        return CatalogTable.of(
                TableIdentifier.of("test", "db", "schema", "table"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, null, null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name",
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

    private SeaTunnelRow row(int id, String name) {
        SeaTunnelRow row = new SeaTunnelRow(2);
        row.setField(0, id);
        row.setField(1, name);
        return row;
    }

    private static final class FakeLLMModel implements Model {
        @Override
        public List<String> inference(List<SeaTunnelRow> rows) {
            List<String> outputs = new ArrayList<>(rows.size());
            for (SeaTunnelRow row : rows) {
                outputs.add("result-" + row.getField(0));
            }
            return outputs;
        }

        @Override
        public void close() throws IOException {}
    }
}
