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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.sink;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.ProducerConfig;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.ACL_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.NAME_SRV_ADDR;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.SECRET_KEY;

public class RocketMqSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private static final String DEFAULT_PRODUCER_GROUP = "SeaTunnel-Producer-Group";
    private SeaTunnelRowType seaTunnelRowType;
    private ProducerMetadata producerMetadata;

    public RocketMqSink(ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.initProducerMetadata(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return "Rocketmq";
    }

    private void initProducerMetadata(ReadonlyConfig pluginConfig) {
        producerMetadata = new ProducerMetadata();
        producerMetadata.setTopic(pluginConfig.get(ProducerConfig.TOPIC));
        RocketMqBaseConfiguration.Builder baseConfigurationBuilder =
                RocketMqBaseConfiguration.newBuilder()
                        .producer()
                        .namesrvAddr(pluginConfig.get(NAME_SRV_ADDR));
        // acl config
        Boolean aclEnabled = pluginConfig.get(ACL_ENABLED);
        if (aclEnabled) {
            if ((!pluginConfig.getOptional(ACCESS_KEY).isPresent()
                    || !pluginConfig.getOptional(SECRET_KEY).isPresent())) {
                throw new RocketMqConnectorException(
                        SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                        "When ACL_ENABLED "
                                + "true , ACCESS_KEY and SECRET_KEY must be configured");
            }
            baseConfigurationBuilder.accessKey(pluginConfig.get(ACCESS_KEY));
            baseConfigurationBuilder.secretKey(pluginConfig.get(SECRET_KEY));
        }
        baseConfigurationBuilder.aclEnable(aclEnabled);

        // config producer group
        String producerGroup =
                pluginConfig
                        .getOptional(ProducerConfig.PRODUCER_GROUP)
                        .orElse(DEFAULT_PRODUCER_GROUP);
        baseConfigurationBuilder.groupId(producerGroup);

        baseConfigurationBuilder.maxMessageSize(pluginConfig.get(ProducerConfig.MAX_MESSAGE_SIZE));
        baseConfigurationBuilder.sendMsgTimeout(
                pluginConfig.get(ProducerConfig.SEND_MESSAGE_TIMEOUT_MILLIS));

        this.producerMetadata.setConfiguration(baseConfigurationBuilder.build());

        producerMetadata.setFormat(SchemaFormat.valueOf(pluginConfig.get(FORMAT).toUpperCase()));

        String fieldDelimiter =
                pluginConfig.getOptional(FIELD_DELIMITER).orElse(DEFAULT_FIELD_DELIMITER);
        producerMetadata.setFieldDelimiter(fieldDelimiter);

        producerMetadata.setPartitionKeyFields(
                pluginConfig.get(ProducerConfig.PARTITION_KEY_FIELDS));

        producerMetadata.setExactlyOnce(pluginConfig.get(ProducerConfig.EXACTLY_ONCE));

        producerMetadata.setSync(pluginConfig.get(ProducerConfig.SEND_SYNC));
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new RocketMqSinkWriter(producerMetadata, seaTunnelRowType);
    }
}
