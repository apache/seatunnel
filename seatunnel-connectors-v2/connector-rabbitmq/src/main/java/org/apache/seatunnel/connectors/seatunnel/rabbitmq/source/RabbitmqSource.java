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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplitEnumeratorState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RabbitmqSource
        implements SeaTunnelSource<SeaTunnelRow, RabbitmqSplit, RabbitmqSplitEnumeratorState>,
                SupportParallelism {

    private JobContext jobContext;
    private final RabbitmqConfig rabbitmqConfig;
    private final List<CatalogTable> catalogTables;

    public RabbitmqSource(ReadonlyConfig config) {
        this.rabbitmqConfig = new RabbitmqConfig(config);
        this.catalogTables = initializeCatalogTables(config);
    }

    private List<CatalogTable> initializeCatalogTables(ReadonlyConfig config) {
        boolean hasTableConfigs =
                config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent();
        boolean hasSchema = config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent();

        if (hasTableConfigs && hasSchema) {
            throw new RabbitmqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Cannot specify both 'table_configs' and 'schema'. Please use 'table_configs' for multi-table or 'schema' for single-table mode.");
        }

        List<CatalogTable> tables = new ArrayList<>();

        // Multi-table configuration
        if (hasTableConfigs) {
            List<Map<String, Object>> tableConfigList =
                    config.get(ConnectorCommonOptions.TABLE_CONFIGS);
            for (Map<String, Object> item : tableConfigList) {
                ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(item);
                // We use our helper to ensure the TableIdentifier matches the queue name
                tables.add(buildCatalogTableWithCorrectId(tableConfig));
            }
        }
        // Legacy Single-table configuration
        else if (hasSchema) {
            tables.add(buildCatalogTableWithCorrectId(config));
        } else {
            throw new RabbitmqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "No 'schema' or 'table_configs' found. Please configure at least one table.");
        }

        return tables;
    }

    /**
     * Helper method to build a CatalogTable where the TableName is explicitly set to the
     * queue_name. This prevents the "default" name issue.
     */
    private CatalogTable buildCatalogTableWithCorrectId(ReadonlyConfig config) {
        CatalogTable table = CatalogTableUtil.buildWithConfig(config);

        String tableName;
        if (config.getOptional(ConnectorCommonOptions.PLUGIN_OUTPUT).isPresent()) {
            tableName = config.get(ConnectorCommonOptions.PLUGIN_OUTPUT);
        } else {
            tableName = config.get(RabbitmqBaseOptions.QUEUE_NAME);
            if (tableName == null || tableName.isEmpty()) {
                tableName = rabbitmqConfig.getQueueName();
            }
        }

        // Reconstruct the CatalogTable with the queue name as the Table Name
        return CatalogTable.of(
                TableIdentifier.of(
                        table.getTableId().getCatalogName(),
                        table.getTableId().getDatabaseName(),
                        tableName),
                table.getTableSchema(),
                table.getOptions(),
                table.getPartitionKeys(),
                table.getComment());
    }

    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null && !JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new RabbitmqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, "not support batch job mode"));
        }
        return rabbitmqConfig.isForE2ETesting() ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "RabbitMQ";
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return catalogTables;
    }

    @Override
    public SourceReader<SeaTunnelRow, RabbitmqSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new RabbitmqSourceReader(catalogTables, readerContext, rabbitmqConfig);
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext) throws Exception {
        return new RabbitmqSplitEnumerator(
                enumeratorContext, rabbitmqConfig, getProducedCatalogTables());
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext,
            RabbitmqSplitEnumeratorState checkpointState)
            throws Exception {
        return new RabbitmqSplitEnumerator(
                enumeratorContext, rabbitmqConfig, catalogTables, checkpointState);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
