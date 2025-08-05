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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarAdminConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.NeverStopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions.CURSOR_STARTUP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions.CURSOR_STOP_MODE;

public class PulsarSource
        implements SeaTunnelSource<SeaTunnelRow, PulsarPartitionSplit, PulsarSplitEnumeratorState>,
                SupportParallelism {

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private CatalogTable catalogTable;

    private PulsarAdminConfig adminConfig;
    private PulsarClientConfig clientConfig;
    private PulsarConsumerConfig consumerConfig;
    private PulsarDiscoverer partitionDiscoverer;
    private long partitionDiscoveryIntervalMs;
    private StartCursor startCursor;
    private StopCursor stopCursor;

    protected int pollTimeout;
    protected long pollInterval;
    protected int batchSize;

    public PulsarSource(ReadonlyConfig config, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        // admin config
        PulsarAdminConfig.Builder adminConfigBuilder =
                PulsarAdminConfig.builder()
                        .adminUrl(config.get(PulsarSourceOptions.ADMIN_SERVICE_URL));
        adminConfigBuilder.authPluginClassName(config.get(PulsarSourceOptions.AUTH_PLUGIN_CLASS));
        adminConfigBuilder.authParams(config.get(PulsarSourceOptions.AUTH_PARAMS));
        this.adminConfig = adminConfigBuilder.build();

        // client config
        PulsarClientConfig.Builder clientConfigBuilder =
                PulsarClientConfig.builder()
                        .serviceUrl(config.get(PulsarSourceOptions.CLIENT_SERVICE_URL));
        clientConfigBuilder.authPluginClassName(config.get(PulsarSourceOptions.AUTH_PLUGIN_CLASS));
        clientConfigBuilder.authParams(config.get(PulsarSourceOptions.AUTH_PARAMS));
        this.clientConfig = clientConfigBuilder.build();

        // consumer config
        PulsarConsumerConfig.Builder consumerConfigBuilder =
                PulsarConsumerConfig.builder()
                        .subscriptionName(config.get(PulsarSourceOptions.SUBSCRIPTION_NAME));
        this.consumerConfig = consumerConfigBuilder.build();

        // source properties
        this.partitionDiscoveryIntervalMs =
                config.get(PulsarSourceOptions.TOPIC_DISCOVERY_INTERVAL);
        this.pollTimeout = config.get(PulsarSourceOptions.POLL_TIMEOUT);
        this.pollInterval = config.get(PulsarSourceOptions.POLL_INTERVAL);
        this.batchSize = config.get(PulsarSourceOptions.POLL_BATCH_SIZE);

        setStartCursor(config);
        setStopCursor(config);
        setPartitionDiscoverer(config);
        setDeserialization(config);

        if (partitionDiscoverer instanceof TopicPatternDiscoverer
                && partitionDiscoveryIntervalMs > 0
                && Boundedness.BOUNDED == stopCursor.getBoundedness()) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Bounded streams do not support dynamic partition discovery.");
        }
    }

    @Override
    public String getPluginName() {
        return PulsarSourceOptions.IDENTIFIER;
    }

    private void setStartCursor(ReadonlyConfig config) {
        PulsarSourceOptions.StartMode startMode = config.get(CURSOR_STARTUP_MODE);
        switch (startMode) {
            case EARLIEST:
                this.startCursor = StartCursor.earliest();
                break;
            case LATEST:
                this.startCursor = StartCursor.latest();
                break;
            case SUBSCRIPTION:
                PulsarSourceOptions.CursorResetStrategy resetStrategy =
                        config.get(PulsarSourceOptions.CURSOR_RESET_MODE);
                this.startCursor = StartCursor.subscription(resetStrategy);
                break;
            case TIMESTAMP:
                if (!config.getOptional(PulsarSourceOptions.CURSOR_STARTUP_TIMESTAMP).isPresent()) {
                    throw new PulsarConnectorException(
                            SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                            String.format(
                                    "The '%s' property is required when the '%s' is 'timestamp'.",
                                    PulsarSourceOptions.CURSOR_STARTUP_TIMESTAMP.key(),
                                    CURSOR_STARTUP_MODE.key()));
                }
                this.startCursor =
                        StartCursor.timestamp(
                                config.get(PulsarSourceOptions.CURSOR_STARTUP_TIMESTAMP));
                break;
            default:
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                        String.format("The %s mode is not supported.", startMode));
        }
    }

    private void setStopCursor(ReadonlyConfig config) {
        PulsarSourceOptions.StopMode stopMode = config.get(CURSOR_STOP_MODE);
        switch (stopMode) {
            case LATEST:
                this.stopCursor = StopCursor.latest();
                break;
            case NEVER:
                this.stopCursor = StopCursor.never();
                break;
            case TIMESTAMP:
                if (!config.getOptional(PulsarSourceOptions.CURSOR_STOP_TIMESTAMP).isPresent()) {
                    throw new PulsarConnectorException(
                            SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                            String.format(
                                    "The '%s' property is required when the '%s' is 'timestamp'.",
                                    PulsarSourceOptions.CURSOR_STOP_TIMESTAMP.key(),
                                    CURSOR_STOP_MODE.key()));
                }
                this.stopCursor =
                        StopCursor.timestamp(config.get(PulsarSourceOptions.CURSOR_STOP_TIMESTAMP));
                break;
            default:
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format("The %s mode is not supported.", stopMode));
        }
    }

    private void setPartitionDiscoverer(ReadonlyConfig config) {
        if (config.getOptional(PulsarSourceOptions.TOPIC).isPresent()) {
            String topic = config.get(PulsarSourceOptions.TOPIC);
            if (StringUtils.isNotBlank(topic)) {
                this.partitionDiscoverer =
                        new TopicListDiscoverer(Arrays.asList(StringUtils.split(topic, ",")));
            }
        }
        if (config.getOptional(PulsarSourceOptions.TOPIC_PATTERN).isPresent()) {
            String topicPattern = config.get(PulsarSourceOptions.TOPIC_PATTERN);
            if (StringUtils.isNotBlank(topicPattern)) {
                this.partitionDiscoverer =
                        new TopicPatternDiscoverer(Pattern.compile(topicPattern));
            }
        }
        if (this.partitionDiscoverer == null) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                    String.format(
                            "The properties '%s' or '%s' is required.",
                            PulsarSourceOptions.TOPIC.key(),
                            PulsarSourceOptions.TOPIC_PATTERN.key()));
        }
    }

    private void setDeserialization(ReadonlyConfig config) {
        String format = config.get(PulsarSourceOptions.FORMAT);
        switch (format.toUpperCase()) {
            case "JSON":
                this.deserializationSchema =
                        new JsonDeserializationSchema(
                                false, false, catalogTable.getSeaTunnelRowType());
                break;
            case "CANAL_JSON":
                this.deserializationSchema =
                        new PulsarCanalDecorator(
                                CanalJsonDeserializationSchema.builder(catalogTable)
                                        .setIgnoreParseErrors(true)
                                        .build());
                break;
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unsupported format: " + format);
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return this.stopCursor instanceof NeverStopCursor
                ? Boundedness.UNBOUNDED
                : Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, PulsarPartitionSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new PulsarSourceReader<>(
                readerContext,
                clientConfig,
                consumerConfig,
                startCursor,
                deserializationSchema,
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
                startCursor,
                stopCursor,
                consumerConfig.getSubscriptionName());
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
                startCursor,
                stopCursor,
                consumerConfig.getSubscriptionName(),
                checkpointState.getAssignedPartitions());
    }
}
