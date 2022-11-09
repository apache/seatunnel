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
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.ADMIN_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PLUGIN_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CLIENT_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_RESET_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STARTUP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STARTUP_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STOP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STOP_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.POLL_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.POLL_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.POLL_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.SUBSCRIPTION_NAME;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.StartMode;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.StartMode.LATEST;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.StopMode.NEVER;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.TOPIC_DISCOVERY_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.TOPIC_PATTERN;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarAdminConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.PulsarSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.SubscriptionStartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.NeverStopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.PulsarDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicListDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicPatternDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader.PulsarSourceReader;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.split.PulsarPartitionSplit;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.regex.Pattern;

@AutoService(SeaTunnelSource.class)
public class PulsarSource<T> implements SeaTunnelSource<T, PulsarPartitionSplit, PulsarSplitEnumeratorState> {
    private DeserializationSchema<T> deserialization;

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
        return PulsarConfigUtil.IDENTIFIER;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, SUBSCRIPTION_NAME.key(), CLIENT_SERVICE_URL.key(), ADMIN_SERVICE_URL.key());
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }

        // admin config
        PulsarAdminConfig.Builder adminConfigBuilder = PulsarAdminConfig.builder()
            .adminUrl(config.getString(ADMIN_SERVICE_URL.key()));
        setOption(config, AUTH_PLUGIN_CLASS.key(), config::getString, adminConfigBuilder::authPluginClassName);
        setOption(config, AUTH_PARAMS.key(), config::getString, adminConfigBuilder::authParams);
        this.adminConfig = adminConfigBuilder.build();

        // client config
        PulsarClientConfig.Builder clientConfigBuilder = PulsarClientConfig.builder()
            .serviceUrl(config.getString(CLIENT_SERVICE_URL.key()));
        setOption(config, AUTH_PLUGIN_CLASS.key(), config::getString, clientConfigBuilder::authPluginClassName);
        setOption(config, AUTH_PARAMS.key(), config::getString, clientConfigBuilder::authParams);
        this.clientConfig = clientConfigBuilder.build();

        // consumer config
        PulsarConsumerConfig.Builder consumerConfigBuilder = PulsarConsumerConfig.builder()
            .subscriptionName(config.getString(SUBSCRIPTION_NAME.key()));
        this.consumerConfig = consumerConfigBuilder.build();

        // source properties
        setOption(config,
            TOPIC_DISCOVERY_INTERVAL.key(),
            TOPIC_DISCOVERY_INTERVAL.defaultValue(),
            config::getLong,
            v -> this.partitionDiscoveryIntervalMs = v);
        setOption(config,
            POLL_TIMEOUT.key(),
            POLL_TIMEOUT.defaultValue(),
            config::getInt,
            v -> this.pollTimeout = v);
        setOption(config,
            POLL_INTERVAL.key(),
            POLL_INTERVAL.defaultValue(),
            config::getLong,
            v -> this.pollInterval = v);
        setOption(config,
            POLL_BATCH_SIZE.key(),
            POLL_BATCH_SIZE.defaultValue(),
            config::getInt,
            v -> this.batchSize = v);

        setStartCursor(config);
        setStopCursor(config);
        setPartitionDiscoverer(config);
        setDeserialization(config);

        if (partitionDiscoverer instanceof TopicPatternDiscoverer
            && partitionDiscoveryIntervalMs > 0
            && Boundedness.BOUNDED == stopCursor.getBoundedness()) {
            throw new IllegalArgumentException("Bounded streams do not support dynamic partition discovery.");
        }
    }

    private void setStartCursor(Config config) {
        StartMode startMode = getEnum(config, CURSOR_STARTUP_MODE.key(), StartMode.class, CURSOR_STARTUP_MODE.defaultValue());
        switch (startMode) {
            case EARLIEST:
                this.startCursor = StartCursor.earliest();
                break;
            case LATEST:
                this.startCursor = StartCursor.latest();
                break;
            case SUBSCRIPTION:
                SubscriptionStartCursor.CursorResetStrategy resetStrategy = getEnum(config,
                    CURSOR_RESET_MODE.key(),
                    SubscriptionStartCursor.CursorResetStrategy.class,
                    SubscriptionStartCursor.CursorResetStrategy.LATEST);
                this.startCursor = StartCursor.subscription(resetStrategy);
                break;
            case TIMESTAMP:
                if (StringUtils.isBlank(config.getString(CURSOR_STARTUP_TIMESTAMP.key()))) {
                    throw new IllegalArgumentException(String.format("The '%s' property is required when the '%s' is 'timestamp'.", CURSOR_STARTUP_TIMESTAMP.key(), CURSOR_STARTUP_MODE.key()));
                }
                setOption(config, CURSOR_STARTUP_TIMESTAMP.key(), config::getLong, timestamp -> this.startCursor = StartCursor.timestamp(timestamp));
                break;
            default:
                throw new IllegalArgumentException(String.format("The %s mode is not supported.", startMode));
        }
    }

    private void setStopCursor(Config config) {
        SourceProperties.StopMode stopMode = getEnum(config, CURSOR_STOP_MODE.key(), SourceProperties.StopMode.class, CURSOR_STOP_MODE.defaultValue());
        switch (stopMode) {
            case LATEST:
                this.stopCursor = StopCursor.latest();
                break;
            case NEVER:
                this.stopCursor = StopCursor.never();
                break;
            case TIMESTAMP:
                if (StringUtils.isBlank(config.getString(CURSOR_STOP_TIMESTAMP.key()))) {
                    throw new IllegalArgumentException(String.format("The '%s' property is required when the '%s' is 'timestamp'.", CURSOR_STOP_TIMESTAMP.key(), CURSOR_STOP_MODE.key()));
                }
                setOption(config, CURSOR_STARTUP_TIMESTAMP.key(), config::getLong, timestamp -> this.stopCursor = StopCursor.timestamp(timestamp));
                break;
            default:
                throw new IllegalArgumentException(String.format("The %s mode is not supported.", stopMode));
        }
    }

    private void setPartitionDiscoverer(Config config) {
        String topic = config.getString(TOPIC.key());
        if (StringUtils.isNotBlank(topic)) {
            this.partitionDiscoverer = new TopicListDiscoverer(Arrays.asList(StringUtils.split(topic, ",")));
        }
        String topicPattern = config.getString(TOPIC_PATTERN.key());
        if (StringUtils.isNotBlank(topicPattern)) {
            if (this.partitionDiscoverer != null) {
                throw new IllegalArgumentException(String.format("The properties '%s' and '%s' is exclusive.", TOPIC.key(), TOPIC_PATTERN.key()));
            }
            this.partitionDiscoverer = new TopicPatternDiscoverer(Pattern.compile(topicPattern));
        }
        if (this.partitionDiscoverer == null) {
            throw new IllegalArgumentException(String.format("The properties '%s' or '%s' is required.", TOPIC.key(), TOPIC_PATTERN.key()));
        }
    }

    private void setDeserialization(Config config) {
        String format = config.getString("format");
        // TODO: format SPI
        SeaTunnelRowType rowType = SeaTunnelSchema.buildWithConfig(config.getConfig(SeaTunnelSchema.SCHEMA.key())).getSeaTunnelRowType();
        deserialization = (DeserializationSchema<T>) new JsonDeserializationSchema(false, false, rowType);
    }

    @Override
    public Boundedness getBoundedness() {
        return this.stopCursor instanceof NeverStopCursor ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<T> getProducedType() {
        return deserialization.getProducedType();
    }

    @Override
    public SourceReader<T, PulsarPartitionSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new PulsarSourceReader<>(readerContext,
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

}
