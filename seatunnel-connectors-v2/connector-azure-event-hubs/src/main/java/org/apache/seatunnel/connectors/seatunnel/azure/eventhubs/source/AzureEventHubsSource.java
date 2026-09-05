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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/** Native AMQP source for one Azure Event Hub. */
public class AzureEventHubsSource
        implements SeaTunnelSource<
                        SeaTunnelRow, AzureEventHubsSourceSplit, AzureEventHubsSourceState>,
                SupportParallelism {

    private final ReadonlyConfig readonlyConfig;
    private final AzureEventHubsSourceConfig config;
    private final CatalogTable catalogTable;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final EventHubsConsumerFactory consumerFactory;

    private JobContext jobContext;

    public AzureEventHubsSource(
            ReadonlyConfig readonlyConfig,
            AzureEventHubsSourceConfig config,
            CatalogTable catalogTable,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this(
                readonlyConfig,
                config,
                catalogTable,
                deserializationSchema,
                AzureEventHubsConsumer::new);
    }

    AzureEventHubsSource(
            ReadonlyConfig readonlyConfig,
            AzureEventHubsSourceConfig config,
            CatalogTable catalogTable,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            EventHubsConsumerFactory consumerFactory) {
        this.readonlyConfig = readonlyConfig;
        this.config = config;
        this.catalogTable = catalogTable;
        this.deserializationSchema = deserializationSchema;
        this.consumerFactory = consumerFactory;
    }

    @Override
    public String getPluginName() {
        return AzureEventHubsSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null && !JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.CONFIGURATION_FAILED,
                    "Azure Event Hubs source supports streaming jobs only");
        }
        return Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    /** Creates a reader whose checkpoint position advances only after successful emission. */
    @Override
    public SourceReader<SeaTunnelRow, AzureEventHubsSourceSplit> createReader(
            SourceReader.Context readerContext) {
        Supplier<SplitReader<EventHubsRecord, AzureEventHubsSourceSplit>> splitReaderSupplier =
                () -> new AzureEventHubsSourceSplitReader(config, consumerFactory);
        return new AzureEventHubsSourceReader(
                splitReaderSupplier,
                new AzureEventHubsRecordEmitter(deserializationSchema),
                new SourceReaderOptions(readonlyConfig),
                readerContext);
    }

    /** Discovers partitions and resolves their configured startup positions for a fresh job. */
    @Override
    public SourceSplitEnumerator<AzureEventHubsSourceSplit, AzureEventHubsSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<AzureEventHubsSourceSplit> enumeratorContext) {
        return new AzureEventHubsSourceSplitEnumerator(
                config, enumeratorContext, consumerFactory, null);
    }

    /** Restores checkpointed splits without rediscovering partitions or reapplying start mode. */
    @Override
    public SourceSplitEnumerator<AzureEventHubsSourceSplit, AzureEventHubsSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<AzureEventHubsSourceSplit> enumeratorContext,
                    AzureEventHubsSourceState checkpointState) {
        return new AzureEventHubsSourceSplitEnumerator(
                config, enumeratorContext, consumerFactory, checkpointState);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
