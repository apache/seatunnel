/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplitEnumeratorState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The source implementation for RabbitMQ. Supports both Single-Table (reading from one queue) and
 * Multi-Table (reading from multiple queues simultaneously).
 */
public class RabbitmqSource
        implements SeaTunnelSource<SeaTunnelRow, RabbitmqSplit, RabbitmqSplitEnumeratorState>,
                SupportParallelism {

    private JobContext jobContext;
    private final RabbitmqConfig rabbitmqConfig;
    // The list of catalog tables that this source will produce
    private final List<CatalogTable> catalogTables;
    // Maps the Split ID (usually the queue name or plugin_output) to its corresponding CatalogTable
    private final Map<String, CatalogTable> queueToTableMap;

    public RabbitmqSource(ReadonlyConfig config) {
        this.rabbitmqConfig = new RabbitmqConfig(config);
        this.catalogTables = new ArrayList<>();
        this.queueToTableMap = new HashMap<>();
        initializeCatalogTables(config);
    }

    /**
     * Parses the configuration to initialize the CatalogTables. Determines whether the source is
     * operating in Single-Table or Multi-Table mode.
     *
     * @param config The plugin configuration.
     */
    private void initializeCatalogTables(ReadonlyConfig config) {
        boolean hasTableConfigs =
                config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent();
        boolean hasSchema = config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent();

        // Mutually exclusive check: Users cannot define both root-level schema and table_configs
        if (hasTableConfigs && hasSchema) {
            throw new RabbitmqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Cannot specify both 'table_configs' and 'schema'.");
        }

        if (hasTableConfigs) {
            // Multi-Table Mode: Parse multiple queue configurations
            List<Map<String, Object>> tableConfigList =
                    config.get(ConnectorCommonOptions.TABLE_CONFIGS);
            for (Map<String, Object> item : tableConfigList) {
                ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(item);
                CatalogTable table = CatalogTableUtil.buildWithConfig(tableConfig);
                String queueName = tableConfig.get(RabbitmqBaseOptions.QUEUE_NAME);

                // Ensure queue_name is explicitly defined
                if (queueName == null || queueName.trim().isEmpty()) {
                    throw new RabbitmqConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                            "The 'queue_name' is missing or empty inside one of the 'table_configs' items.");
                }

                String splitId = queueName;

                this.catalogTables.add(table);
                this.queueToTableMap.put(splitId, table);
            }
        } else if (hasSchema) {
            CatalogTable table = CatalogTableUtil.buildWithConfig(config);
            String queueName = config.get(RabbitmqBaseOptions.QUEUE_NAME);
            if (queueName == null) {
                queueName = rabbitmqConfig.getQueueName();
            }
            this.catalogTables.add(table);
            this.queueToTableMap.put(queueName, table);
        } else {
            throw new RabbitmqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "No 'schema' or 'table_configs' found.");
        }
    }

    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null && !JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new RabbitmqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, Message: not support batch job mode",
                            getPluginName()));
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
        // Pass the pre-computed queueToTableMap so the reader knows how to route each message
        return new RabbitmqSourceReader(queueToTableMap, readerContext, rabbitmqConfig);
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext) throws Exception {
        return new RabbitmqSplitEnumerator(
                enumeratorContext, rabbitmqConfig, new ArrayList<>(queueToTableMap.keySet()));
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext,
            RabbitmqSplitEnumeratorState checkpointState)
            throws Exception {
        return new RabbitmqSplitEnumerator(
                enumeratorContext,
                rabbitmqConfig,
                new ArrayList<>(queueToTableMap.keySet()),
                checkpointState);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
