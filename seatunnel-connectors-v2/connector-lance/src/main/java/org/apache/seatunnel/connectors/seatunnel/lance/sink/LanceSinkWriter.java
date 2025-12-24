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
import java.util.List;
import java.util.Optional;

@Slf4j
public class LanceSinkWriter
        implements SinkWriter<SeaTunnelRow, LanceCommitInfo, LanceSinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {

    private SeaTunnelRowType seaTunnelRowType;

    private TableSchema sourceTableSchema;

    private org.apache.arrow.vector.types.pojo.Schema schema;

    private LanceSinkConfig config;

    private LanceCatalog catalog;

    public LanceSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            TableSchema sourceTableSchema,
            LanceSinkConfig config,
            LanceCatalog catalog) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.sourceTableSchema = sourceTableSchema;
        this.config = config;
        this.catalog = catalog;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            org.apache.arrow.vector.types.pojo.Schema datasetSchema = null;
            try {
                Dataset existingDataset = Dataset.open(config.getDatasetPath(), allocator);
                datasetSchema = existingDataset.getSchema();
                existingDataset.close();
            } catch (Exception e) {
                log.debug("Dataset does not exist, will create new schema: {}", e.getMessage());
            }

            if (datasetSchema != null
                    && datasetSchema.getFields() != null
                    && !datasetSchema.getFields().isEmpty()) {
                this.schema = datasetSchema;
            } else {
                this.schema = SchemaUtils.convertSchema(element, seaTunnelRowType);
            }

            List<FragmentMetadata> fragmentMetadata =
                    FragmentConverter.reconvert(
                            element, seaTunnelRowType, schema, allocator, config.getDatasetPath());

            boolean datasetExists = false;
            try {
                Dataset testDataset = Dataset.open(config.getDatasetPath(), allocator);
                testDataset.close();
                datasetExists = true;
            } catch (Exception e) {
                log.debug(
                        "Dataset does not exist at {}, will create it: {}",
                        config.getDatasetPath(),
                        e.getMessage());
            }

            if (!datasetExists) {
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
                    log.debug("Created new dataset at {}", config.getDatasetPath());
                } catch (Exception e) {
                    throw new LanceConnectorException(
                            LanceConnectorErrorCode.TABLE_DATASET_PATH_OPEN_EXCEPTION,
                            "Failed to create dataset: " + e.getMessage(),
                            e);
                }
            }

            try {
                Dataset dataset = Dataset.open(config.getDatasetPath(), allocator);
                Transaction transaction =
                        dataset.newTransactionBuilder()
                                .operation(Append.builder().fragments(fragmentMetadata).build())
                                .build();
                try (Dataset appendedDataset = transaction.commit()) {
                    log.debug(
                            "LanceSinkWriter commit SeatunnelRow with lance dataset version: "
                                    + appendedDataset.version());
                }
                dataset.close();
            } catch (Exception e) {
                throw new LanceConnectorException(
                        LanceConnectorErrorCode.TABLE_DATASET_PATH_OPEN_EXCEPTION, e.getMessage());
            }
        } catch (Exception e) {
            throw new LanceConnectorException(
                    LanceConnectorErrorCode.TABLE_DATASET_WRITE_ST_ROW_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        SinkWriter.super.applySchemaChange(event);
    }

    @Override
    public Optional<LanceCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {}
}
