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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.lance.catalog.LanceCatalog;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;
import org.apache.seatunnel.connectors.seatunnel.lance.sink.commit.LanceCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.lance.state.LanceSinkState;
import org.apache.seatunnel.connectors.seatunnel.lance.utils.FragmentConverter;
import org.apache.seatunnel.connectors.seatunnel.lance.utils.SchemaUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.Transaction;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.operation.Append;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class LanceSinkWriter
        implements SinkWriter<SeaTunnelRow, LanceCommitInfo, LanceSinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {

    private static final int DEFAULT_BATCH_SIZE = 1000;

    private final SeaTunnelRowType seaTunnelRowType;
    private final TableSchema sourceTableSchema;
    private final LanceSinkConfig config;
    private final LanceCatalog catalog;
    private final int batchSize;

    private BufferAllocator allocator;
    private org.apache.arrow.vector.types.pojo.Schema schema;
    private Dataset dataset;
    private boolean datasetInitialized = false;

    private final List<SeaTunnelRow> batchBuffer;

    public LanceSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            TableSchema sourceTableSchema,
            LanceSinkConfig config,
            LanceCatalog catalog) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.sourceTableSchema = sourceTableSchema;
        this.config = config;
        this.catalog = catalog;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.batchBuffer = new ArrayList<>(batchSize);
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    private void initializeDataset(SeaTunnelRow firstElement) {
        if (datasetInitialized) {
            return;
        }

        try {
            Dataset existingDataset = Dataset.open(config.getDatasetPath(), allocator);
            this.schema = existingDataset.getSchema();
            this.dataset = existingDataset;
            datasetInitialized = true;
        } catch (Exception e) {
            this.schema = SchemaUtils.convertSchema(firstElement, seaTunnelRowType);

            try {
                Dataset.create(
                        allocator,
                        config.getDatasetPath(),
                        schema,
                        new WriteParams.Builder()
                                .withMaxBytesPerFile(config.getMaxBytesPerFile())
                                .withMaxRowsPerFile(config.getMaxRowsPerFile())
                                .withMode(config.getMode())
                                .withStorageOptions(config.getStorageOptions())
                                .build());

                this.dataset = Dataset.open(config.getDatasetPath(), allocator);
                datasetInitialized = true;
            } catch (Exception createEx) {
                throw new LanceConnectorException(
                        LanceConnectorErrorCode.TABLE_DATASET_PATH_OPEN_EXCEPTION,
                        "Failed to create dataset: " + createEx.getMessage(),
                        createEx);
            }
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (!datasetInitialized) {
            initializeDataset(element);
        }

        batchBuffer.add(element);

        if (batchBuffer.size() >= batchSize) {
            flushBatch();
        }
    }

    private void flushBatch() {
        if (batchBuffer.isEmpty()) {
            return;
        }

        try {
            List<FragmentMetadata> allFragments = new ArrayList<>();

            for (SeaTunnelRow row : batchBuffer) {
                List<FragmentMetadata> fragmentMetadata =
                        FragmentConverter.reconvert(
                                row, seaTunnelRowType, schema, allocator, config.getDatasetPath());
                allFragments.addAll(fragmentMetadata);
            }

            if (!allFragments.isEmpty()) {
                Transaction transaction =
                        dataset.newTransactionBuilder()
                                .operation(Append.builder().fragments(allFragments).build())
                                .build();

                try (Dataset appendedDataset = transaction.commit()) {
                    log.debug(
                            "Flushed {} rows to lance dataset, new version: {}",
                            batchBuffer.size(),
                            appendedDataset.version());
                }

                if (dataset != null) {
                    dataset.close();
                }
                dataset = Dataset.open(config.getDatasetPath(), allocator);
            }

            batchBuffer.clear();
        } catch (Exception e) {
            throw new LanceConnectorException(
                    LanceConnectorErrorCode.TABLE_DATASET_WRITE_ST_ROW_EXCEPTION,
                    "Failed to flush batch: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        SinkWriter.super.applySchemaChange(event);
    }

    @Override
    public Optional<LanceCommitInfo> prepareCommit() throws IOException {
        flushBatch();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        batchBuffer.clear();
    }

    @Override
    public void close() throws IOException {
        try {
            flushBatch();
        } finally {
            if (dataset != null) {
                try {
                    dataset.close();
                } catch (Exception e) {
                    log.warn("Failed to close dataset: {}", e.getMessage());
                }
                dataset = null;
            }

            if (allocator != null) {
                try {
                    allocator.close();
                } catch (Exception e) {
                    log.warn("Failed to close allocator: {}", e.getMessage());
                }
                allocator = null;
            }
        }
    }
}
