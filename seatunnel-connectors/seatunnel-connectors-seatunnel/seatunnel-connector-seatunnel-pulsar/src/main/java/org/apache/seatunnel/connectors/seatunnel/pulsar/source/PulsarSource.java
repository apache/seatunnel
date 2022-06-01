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

import static org.apache.seatunnel.common.PropertiesUtil.getEnum;
import static org.apache.seatunnel.common.PropertiesUtil.setOption;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_ADMIN_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_AUTH_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_CURSOR_START_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_CURSOR_START_RESET_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_CURSOR_START_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_CURSOR_STOP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_CURSOR_STOP_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_POLL_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_POLL_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.PULSAR_TOPIC_PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.StartMode;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.StartMode.LATEST;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.StopMode.NEVER;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarAdminConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.SubscriptionStartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.PulsarDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicListDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicPatternDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader.PulsarSourceReader;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader.serializer.PulsarDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.split.PulsarPartitionSplit;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.regex.Pattern;

@AutoService(SeaTunnelSource.class)
public class PulsarSource implements SeaTunnelSource<SeaTunnelRow, PulsarPartitionSplit, PulsarSplitEnumeratorState> {
    public static final String IDENTIFIER = "pulsar";
    private PulsarDeserializationSchema deserialization;
    private SeaTunnelContext seaTunnelContext;

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

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, PULSAR_SUBSCRIPTION_NAME, PULSAR_SERVICE_URL, PULSAR_ADMIN_URL);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }

        // admin config
        PulsarAdminConfig.Builder adminConfigBuilder = PulsarAdminConfig.builder()
            .adminUrl(config.getString(PULSAR_ADMIN_URL));
        setOption(config, PULSAR_AUTH_PLUGIN_CLASS_NAME, config::getString, adminConfigBuilder::authPluginClassName);
        setOption(config, PULSAR_AUTH_PARAMS, config::getString, adminConfigBuilder::authParams);
        this.adminConfig = adminConfigBuilder.build();

        // client config
        PulsarClientConfig.Builder clientConfigBuilder = PulsarClientConfig.builder()
            .serviceUrl(config.getString(PULSAR_SERVICE_URL));
        setOption(config, PULSAR_AUTH_PLUGIN_CLASS_NAME, config::getString, clientConfigBuilder::authPluginClassName);
        setOption(config, PULSAR_AUTH_PARAMS, config::getString, clientConfigBuilder::authParams);
        this.clientConfig = clientConfigBuilder.build();

        // consumer config
        PulsarConsumerConfig.Builder consumerConfigBuilder = PulsarConsumerConfig.builder()
            .subscriptionName(config.getString(PULSAR_SERVICE_URL));
        this.consumerConfig = consumerConfigBuilder.build();

        // source properties
        setOption(config,
            PULSAR_PARTITION_DISCOVERY_INTERVAL_MS,
            30000L,
            config::getLong,
            v -> this.partitionDiscoveryIntervalMs = v);
        setOption(config,
            PULSAR_POLL_TIMEOUT,
            100,
            config::getInt,
            v -> this.pollTimeout = v);
        setOption(config,
            PULSAR_POLL_INTERVAL,
            50L,
            config::getLong,
            v -> this.pollInterval = v);
        setOption(config,
            PULSAR_BATCH_SIZE,
            500,
            config::getInt,
            v -> this.batchSize = v);

        setStartCursor(config);
        setStopCursor(config);
        setPartitionDiscoverer(config);

        if ((partitionDiscoverer instanceof TopicPatternDiscoverer)
            && partitionDiscoveryIntervalMs > 0
            && Boundedness.BOUNDED == stopCursor.getBoundedness()) {
            throw new IllegalArgumentException("Bounded streams do not support dynamic partition discovery.");
        }
    }

    private void setStartCursor(Config config) {
        StartMode startMode = getEnum(config, PULSAR_CURSOR_START_MODE, StartMode.class, LATEST);
        switch (startMode) {
            case EARLIEST:
                this.startCursor = StartCursor.earliest();
                break;
            case LATEST:
                this.startCursor = StartCursor.latest();
                break;
            case SUBSCRIPTION:
                SubscriptionStartCursor.CursorResetStrategy resetStrategy = getEnum(config,
                    PULSAR_CURSOR_START_RESET_MODE,
                    SubscriptionStartCursor.CursorResetStrategy.class,
                    SubscriptionStartCursor.CursorResetStrategy.LATEST);
                this.startCursor = StartCursor.subscription(resetStrategy);
                break;
            case TIMESTAMP:
                if (StringUtils.isBlank(config.getString(PULSAR_CURSOR_START_TIMESTAMP))) {
                    throw new IllegalArgumentException(String.format("The '%s' property is required when the '%s' is 'timestamp'.", PULSAR_CURSOR_START_TIMESTAMP, PULSAR_CURSOR_START_MODE));
                }
                setOption(config, PULSAR_CURSOR_START_TIMESTAMP, config::getLong, timestamp -> this.startCursor = StartCursor.timestamp(timestamp));
                break;
            default:
                throw new IllegalArgumentException(String.format("The %s mode is not supported.", startMode));
        }
    }

    private void setStopCursor(Config config) {
        SourceProperties.StopMode stopMode = getEnum(config, PULSAR_CURSOR_STOP_MODE, SourceProperties.StopMode.class, NEVER);
        switch (stopMode) {
            case LATEST:
                this.stopCursor = StopCursor.latest();
                break;
            case NEVER:
                this.stopCursor = StopCursor.never();
                break;
            case TIMESTAMP:
                if (StringUtils.isBlank(config.getString(PULSAR_CURSOR_STOP_TIMESTAMP))) {
                    throw new IllegalArgumentException(String.format("The '%s' property is required when the '%s' is 'timestamp'.", PULSAR_CURSOR_STOP_TIMESTAMP, PULSAR_CURSOR_STOP_MODE));
                }
                setOption(config, PULSAR_CURSOR_START_TIMESTAMP, config::getLong, timestamp -> this.stopCursor = StopCursor.timestamp(timestamp));
                break;
            default:
                throw new IllegalArgumentException(String.format("The %s mode is not supported.", stopMode));
        }
    }

    private void setPartitionDiscoverer(Config config) {
        String topic = config.getString(PULSAR_TOPIC);
        if (StringUtils.isNotBlank(topic)) {
            this.partitionDiscoverer = new TopicListDiscoverer(Arrays.asList(StringUtils.split(topic, ",")));
        }
        String topicPattern = config.getString(PULSAR_TOPIC_PATTERN);
        if (StringUtils.isNotBlank(topicPattern)) {
            if (this.partitionDiscoverer != null) {
                throw new IllegalArgumentException(String.format("The properties '%s' and '%s' is exclusive.", PULSAR_TOPIC, PULSAR_TOPIC_PATTERN));
            }
            this.partitionDiscoverer = new TopicPatternDiscoverer(Pattern.compile(topicPattern));
        }
        if (this.partitionDiscoverer == null) {
            throw new IllegalArgumentException(String.format("The properties '%s' or '%s' is required.", PULSAR_TOPIC, PULSAR_TOPIC_PATTERN));
        }
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return this.seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        return deserialization.getProducedType();
    }

    @Override
    public SourceReader<SeaTunnelRow, PulsarPartitionSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new PulsarSourceReader(readerContext,
            clientConfig,
            consumerConfig,
            startCursor,
            deserialization,
            pollTimeout,
            pollInterval,
            batchSize);
    }

    @Override
    public SourceSplitEnumerator<PulsarPartitionSplit, PulsarSplitEnumeratorState> createEnumerator(SourceSplitEnumerator.Context<PulsarPartitionSplit> enumeratorContext) throws Exception {
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
    public SourceSplitEnumerator<PulsarPartitionSplit, PulsarSplitEnumeratorState> restoreEnumerator(SourceSplitEnumerator.Context<PulsarPartitionSplit> enumeratorContext, PulsarSplitEnumeratorState checkpointState) throws Exception {
        return new PulsarSplitEnumerator(
            enumeratorContext,
            adminConfig,
            partitionDiscoverer,
            partitionDiscoveryIntervalMs,
            startCursor,
            stopCursor,
            consumerConfig.getSubscriptionName(),
            checkpointState.assignedPartitions());
    }

    @Override
    public Serializer<PulsarSplitEnumeratorState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }
}
