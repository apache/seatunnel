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

package org.apache.seatunnel.connectors.seatunnel.lance.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.lance.catalog.LanceCatalog;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkOptions;

import org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import com.lancedb.lance.Dataset;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@DisabledOnOs(OS.WINDOWS)
class LanceSinkWriterBatchTest {

    @TempDir Path tempDir;

    @Test
    void flushWritesOneFragmentPerBatch() throws IOException {
        String datasetPath = tempDir.resolve("batch_fragment.lance").toString();
        Map<String, Object> configs = new HashMap<>();
        configs.put(LanceCommonOptions.KEY_DATASET_PATH.key(), datasetPath);
        configs.put(LanceCommonOptions.KEY_NAMESPACE_TYPE.key(), "dir");
        configs.put(LanceCommonOptions.KEY_ROOT_NAMESPACE_PATH.key(), tempDir.toString());
        configs.put(LanceSinkOptions.WRITE_BATCH_SIZE.key(), 2);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configs);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "c_string",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "c_string"))
                        .column(
                                PhysicalColumn.of(
                                        "c_int",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "c_int"))
                        .build();

        LanceSinkWriter writer =
                new LanceSinkWriter(
                        tableSchema.toPhysicalRowDataType(),
                        tableSchema,
                        new LanceSinkConfig(config),
                        new LanceCatalog("lance_namespace", config));
        for (int i = 0; i < 5; i++) {
            writer.write(new SeaTunnelRow(new Object[] {"row-" + i, i}));
        }
        writer.prepareCommit();
        writer.close();

        try (RootAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(datasetPath, allocator)) {
            Assertions.assertEquals(5, dataset.countRows());
            // Two full batches of 2 rows, then the checkpoint flushes the remaining row.
            Assertions.assertEquals(3, dataset.getFragments().size());
        }
    }
}
