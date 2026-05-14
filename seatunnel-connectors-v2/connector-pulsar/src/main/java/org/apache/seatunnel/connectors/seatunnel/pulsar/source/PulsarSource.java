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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarAdminConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarMultiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarTableConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.NeverStopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.MultiTablePartitionDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.PulsarDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicListDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicPatternDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.format.PulsarCanalDecorator;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader.PulsarSourceReader;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.split.PulsarPartitionSplit;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PulsarSource
        implements SeaTunnelSource<SeaTunnelRow, PulsarPartitionSplit, PulsarSplitEnumeratorState>,
                SupportParallelism {

    private final PulsarMultiTableConfig multiTableConfig;
    private final Map<TablePath, PulsarConsumerMetadata> consumerMetadataMap;
    private final PulsarAdminConfig adminConfig;
    private final PulsarClientConfig clientConfig;
    private final PulsarDiscoverer partitionDiscoverer;

    private final long partitionDiscoveryIntervalMs;
    protected final int pollTimeout;
    protected final long pollInterval;
    protected final int batchSize;

    public PulsarSource(ReadonlyConfig config, CatalogTable catalogTable) {
        this.multiTableConfig = PulsarMultiTableConfig.of(config);
        this.partitionDiscoveryIntervalMs =
                config.get(PulsarSourceOptions.TOPIC_DISCOVERY_INTERVAL);
        this.adminConfig = buildAdminConfig(config);
        this.clientConfig = buildClientConfig(config);
        this.consumerMetadataMap = createConsumerMetadata(catalogTable);
        this.partitionDiscoverer = createPartitionDiscoverer();
        this.pollTimeout = config.get(PulsarSourceOptions.POLL_TIMEOUT);
        this.pollInterval = config.get(PulsarSourceOptions.POLL_INTERVAL);
        this.batchSize = config.get(PulsarSourceOptions.POLL_BATCH_SIZE);

        validateBoundedDiscovery();
    }

    @Override
    public Boundedness getBoundedness() {
        return consumerMetadataMap.values().stream()
                        .map(PulsarConsumerMetadata::getStopCursor)
                        .anyMatch(stopCursor -> stopCursor instanceof NeverStopCursor)
                ? Boundedness.UNBOUNDED
                : Boundedness.BOUNDED;
    }

    @Override
    public String getPluginName() {
        return PulsarSourceOptions.IDENTIFIER;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> catalogTables = new ArrayList<>();
        consumerMetadataMap
                .values()
                .forEach(metadata -> catalogTables.add(metadata.getCatalogTable()));
        return catalogTables;
    }

    @Override
    public SourceReader<SeaTunnelRow, PulsarPartitionSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new PulsarSourceReader<>(
                readerContext,
                clientConfig,
                consumerMetadataMap,
                multiTableConfig.isTablesConfigs(),
                pollTimeout,
                pollInterval,
                batchSize);
    }

    @Override
    public SourceSplitEnumerator<PulsarPartitionSplit, PulsarSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<PulsarPartitionSplit> enumeratorContext)
            throws Exception {
        return new PulsarSplitEnumerator(
                enumeratorContext,
                adminConfig,
                partitionDiscoverer,
                partitionDiscoveryIntervalMs,
                consumerMetadataMap,
                getBoundedness());
    }

    @Override
    public SourceSplitEnumerator<PulsarPartitionSplit, PulsarSplitEnumeratorState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<PulsarPartitionSplit> enumeratorContext,
                    PulsarSplitEnumeratorState checkpointState)
                    throws Exception {
        return new PulsarSplitEnumerator(
                enumeratorContext,
                adminConfig,
                partitionDiscoverer,
                partitionDiscoveryIntervalMs,
                consumerMetadataMap,
                getBoundedness(),
                checkpointState.getAssignedPartitions());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        if (multiTableConfig.isMultiTable()
                && JobMode.BATCH.equals(jobContext.getJobMode())
                && getBoundedness() == Boundedness.UNBOUNDED) {
            List<String> unboundedTables =
                    consumerMetadataMap.entrySet().stream()
                            .filter(e -> e.getValue().getStopCursor() instanceof NeverStopCursor)
                            .map(e -> e.getKey().toString())
                            .collect(Collectors.toList());
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Pulsar source does not support unbounded multi-table configuration in batch mode. "
                                    + "The following tables use cursor.stop.mode=NEVER (unbounded): %s. "
                                    + "Either change them to a bounded mode (LATEST or TIMESTAMP) or use STREAMING mode.",
                            String.join(", ", unboundedTables)));
        }
    }

    private Map<TablePath, PulsarConsumerMetadata> createConsumerMetadata(
            CatalogTable singleCatalogTable) {
        Map<TablePath, PulsarConsumerMetadata> metadataMap = new LinkedHashMap<>();
        for (PulsarTableConfig tableConfig : multiTableConfig.getTableConfigs()) {
            CatalogTable catalogTable =
                    multiTableConfig.isTablesConfigs()
                            ? buildCatalogTable(tableConfig)
                            : CatalogTable.of(
                                    TableIdentifier.of(
                                            PulsarSourceOptions.IDENTIFIER,
                                            singleCatalogTable.getTableId().toTablePath()),
                                    singleCatalogTable);
            TablePath tablePath = catalogTable.getTableId().toTablePath();
            metadataMap.put(
                    tablePath,
                    new PulsarConsumerMetadata(
                            tablePath,
                            catalogTable,
                            createDeserialization(tableConfig.getFormat(), catalogTable),
                            createDiscoverer(tableConfig),
                            createStartCursor(tableConfig),
                            createStopCursor(tableConfig),
                            buildConsumerConfig(tableConfig.getSubscriptionName())));
        }
        return metadataMap;
    }

    private PulsarDiscoverer createPartitionDiscoverer() {
        if (consumerMetadataMap.size() == 1) {
            return consumerMetadataMap.values().iterator().next().getDiscoverer();
        }

        // Preserve tables_configs order so multi-table topic-pattern overlaps are resolved
        // deterministically by MultiTablePartitionDiscoverer.
        List<MultiTablePartitionDiscoverer.TableDiscovererPair> discovererPairs = new ArrayList<>();
        for (PulsarConsumerMetadata metadata : consumerMetadataMap.values()) {
            discovererPairs.add(
                    new MultiTablePartitionDiscoverer.TableDiscovererPair(
                            metadata.getTablePath(),
                            metadata.getDiscoverer(),
                            metadata.getDiscoverer() instanceof TopicPatternDiscoverer));
        }
        return new MultiTablePartitionDiscoverer(discovererPairs);
    }

    private CatalogTable buildCatalogTable(PulsarTableConfig tableConfig) {
        CatalogTable catalogTable;
        if (tableConfig.getSchemaConfig().getOptional(PulsarSourceOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(tableConfig.getSchemaConfig());
        } else {
            catalogTable = CatalogTableUtil.buildSimpleTextTable();
        }

        return CatalogTable.of(
                TableIdentifier.of(PulsarSourceOptions.IDENTIFIER, tableConfig.getTablePath()),
                catalogTable);
    }

    private PulsarAdminConfig buildAdminConfig(ReadonlyConfig config) {
        PulsarAdminConfig.Builder builder =
                PulsarAdminConfig.builder()
                        .adminUrl(config.get(PulsarSourceOptions.ADMIN_SERVICE_URL));
        builder.authPluginClassName(config.get(PulsarSourceOptions.AUTH_PLUGIN_CLASS));
        builder.authParams(config.get(PulsarSourceOptions.AUTH_PARAMS));
        return builder.build();
    }

    private PulsarClientConfig buildClientConfig(ReadonlyConfig config) {
        PulsarClientConfig.Builder builder =
                PulsarClientConfig.builder()
                        .serviceUrl(config.get(PulsarSourceOptions.CLIENT_SERVICE_URL));
        builder.authPluginClassName(config.get(PulsarSourceOptions.AUTH_PLUGIN_CLASS));
        builder.authParams(config.get(PulsarSourceOptions.AUTH_PARAMS));
        return builder.build();
    }

    private PulsarConsumerConfig buildConsumerConfig(String subscriptionName) {
        return PulsarConsumerConfig.builder().subscriptionName(subscriptionName).build();
    }

    private DeserializationSchema<SeaTunnelRow> createDeserialization(
            String format, CatalogTable catalogTable) {
        switch (format.toUpperCase()) {
            case "JSON":
                return new JsonDeserializationSchema(
                        false, false, catalogTable.getSeaTunnelRowType());
            case "CANAL_JSON":
                return new PulsarCanalDecorator(
                        CanalJsonDeserializationSchema.builder(catalogTable)
                                .setIgnoreParseErrors(true)
                                .build());
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }

    private void validateBoundedDiscovery() {
        boolean hasTopicPattern;
        if (partitionDiscoverer instanceof TopicPatternDiscoverer) {
            hasTopicPattern = true;
        } else if (partitionDiscoverer instanceof MultiTablePartitionDiscoverer) {
            hasTopicPattern =
                    ((MultiTablePartitionDiscoverer) partitionDiscoverer).hasTopicPattern();
        } else {
            hasTopicPattern = false;
        }

        if (hasTopicPattern
                && partitionDiscoveryIntervalMs > 0
                && Boundedness.BOUNDED == getBoundedness()) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Bounded streams do not support dynamic partition discovery.");
        }
    }

    private StartCursor createStartCursor(PulsarTableConfig tableConfig) {
        PulsarSourceOptions.StartMode startMode = tableConfig.getStartMode();
        switch (startMode) {
            case EARLIEST:
                return StartCursor.earliest();
            case LATEST:
                return StartCursor.latest();
            case SUBSCRIPTION:
                return StartCursor.subscription(tableConfig.getResetMode());
            case TIMESTAMP:
                return StartCursor.timestamp(tableConfig.getStartTimestamp());
            default:
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                        "Unsupported start mode: " + startMode);
        }
    }

    private StopCursor createStopCursor(PulsarTableConfig tableConfig) {
        PulsarSourceOptions.StopMode stopMode = tableConfig.getStopMode();
        switch (stopMode) {
            case LATEST:
                return StopCursor.latest();
            case NEVER:
                return StopCursor.never();
            case TIMESTAMP:
                return StopCursor.timestamp(tableConfig.getStopTimestamp());
            default:
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "Unsupported stop mode: " + stopMode);
        }
    }

    private PulsarDiscoverer createDiscoverer(PulsarTableConfig tableConfig) {
        if (StringUtils.isNotBlank(tableConfig.getTopic())) {
            return new TopicListDiscoverer(
                    Arrays.asList(StringUtils.split(tableConfig.getTopic(), ",")));
        }
        if (tableConfig.getTopicPattern() != null) {
            return new TopicPatternDiscoverer(tableConfig.getTopicPattern());
        }
        throw new PulsarConnectorException(
                SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                String.format(
                        "The properties '%s' or '%s' is required.",
                        PulsarSourceOptions.TOPIC.key(), PulsarSourceOptions.TOPIC_PATTERN.key()));
    }
}
