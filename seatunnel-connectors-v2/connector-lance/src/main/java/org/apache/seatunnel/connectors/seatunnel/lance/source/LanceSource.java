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

package org.apache.seatunnel.connectors.seatunnel.lance.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;
import org.apache.seatunnel.connectors.seatunnel.lance.utils.ArrowRowConverter;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.lancedb.lance.Dataset;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Batch source that reads a lance dataset from the same locations the sink writes to (local
 * filesystem or object storage such as S3/MinIO). One split per dataset fragment.
 */
@Slf4j
public class LanceSource
        implements SeaTunnelSource<SeaTunnelRow, LanceSourceSplit, LanceSourceState>,
                SupportParallelism {

    private final LanceSourceConfig config;

    private final CatalogTable producedTable;

    public LanceSource(ReadonlyConfig sourceConfig) {
        this.config = new LanceSourceConfig(sourceConfig);
        this.producedTable = buildProducedTable(this.config);
    }

    /** Opens the dataset once to derive the produced catalog table from its Arrow schema. */
    private CatalogTable buildProducedTable(LanceSourceConfig config) {
        try (Dataset dataset = Dataset.open(config.getDatasetPath(), config.toReadOptions())) {
            Schema arrowSchema = dataset.getSchema();
            List<Field> fields = arrowSchema.getFields();
            List<PhysicalColumn> columns = new ArrayList<>(fields.size());
            for (Field field : fields) {
                SeaTunnelDataType<?> dataType = ArrowRowConverter.toSeaTunnelType(field);
                columns.add(
                        PhysicalColumn.of(
                                field.getName(),
                                dataType,
                                (Long) null,
                                field.isNullable(),
                                null,
                                null));
            }
            TableSchema tableSchema =
                    TableSchema.builder().columns(new ArrayList<>(columns)).build();
            TableIdentifier tableId =
                    TableIdentifier.of(
                            LanceCommonConfig.CONNECTOR_IDENTITY, "", "", resolveTableName(config));
            return CatalogTable.of(
                    tableId, tableSchema, Collections.emptyMap(), new ArrayList<>(), null);
        } catch (Exception e) {
            throw new LanceConnectorException(
                    LanceConnectorErrorCode.TABLE_DATASET_PATH_OPEN_EXCEPTION,
                    String.format(
                            "Failed to open lance dataset %s: %s",
                            config.getDatasetPath(), e.getMessage()),
                    e);
        }
    }

    private static String resolveTableName(LanceSourceConfig config) {
        if (config.getTable() != null && !config.getTable().isEmpty()) {
            return config.getTable();
        }
        String path = config.getDatasetPath();
        int slash = path.lastIndexOf('/');
        return slash >= 0 ? path.substring(slash + 1) : path;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(producedTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, LanceSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new LanceSourceReader(readerContext, config, producedTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<LanceSourceSplit, LanceSourceState> createEnumerator(
            SourceSplitEnumerator.Context<LanceSourceSplit> context) throws Exception {
        return new LanceSourceSplitEnumerator(context, config, null);
    }

    @Override
    public SourceSplitEnumerator<LanceSourceSplit, LanceSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<LanceSourceSplit> context,
            LanceSourceState checkpointState)
            throws Exception {
        return new LanceSourceSplitEnumerator(context, config, checkpointState);
    }

    @Override
    public String getPluginName() {
        return LanceCommonConfig.CONNECTOR_IDENTITY;
    }
}
