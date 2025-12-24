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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.RocketMqSinkOptions;

import java.io.IOException;
import java.util.Optional;

public class RocketMqSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final CatalogTable catalogTable;
    private final ProducerMetadata producerMetadata;

    public RocketMqSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        producerMetadata = new ProducerMetadata();
        producerMetadata.setTopic(pluginConfig.get(RocketMqSinkOptions.TOPIC));
        if (pluginConfig.getOptional(RocketMqSinkOptions.TAG).isPresent()) {
            producerMetadata.setTag(pluginConfig.get(RocketMqSinkOptions.TAG));
        }
        RocketMqBaseConfiguration.Builder baseConfigurationBuilder =
                RocketMqBaseConfiguration.newBuilder()
                        .producer()
                        .namesrvAddr(pluginConfig.get(RocketMqSinkOptions.NAME_SRV_ADDR));
        baseConfigurationBuilder.aclEnable(pluginConfig.get(RocketMqSinkOptions.ACL_ENABLED));
        if (pluginConfig.getOptional(RocketMqSinkOptions.ACCESS_KEY).isPresent()) {
            baseConfigurationBuilder.accessKey(pluginConfig.get(RocketMqSinkOptions.ACCESS_KEY));
        }
        if (pluginConfig.getOptional(RocketMqSinkOptions.SECRET_KEY).isPresent()) {
            baseConfigurationBuilder.secretKey(pluginConfig.get(RocketMqSinkOptions.SECRET_KEY));
        }
        baseConfigurationBuilder.groupId(pluginConfig.get(RocketMqSinkOptions.PRODUCER_GROUP));
        baseConfigurationBuilder.maxMessageSize(
                pluginConfig.get(RocketMqSinkOptions.MAX_MESSAGE_SIZE));
        baseConfigurationBuilder.sendMsgTimeout(
                pluginConfig.get(RocketMqSinkOptions.SEND_MESSAGE_TIMEOUT_MILLIS));
        this.producerMetadata.setConfiguration(baseConfigurationBuilder.build());
        producerMetadata.setFormat(pluginConfig.get(RocketMqSinkOptions.FORMAT));
        producerMetadata.setFieldDelimiter(pluginConfig.get(RocketMqSinkOptions.FIELD_DELIMITER));
        if (pluginConfig.getOptional(RocketMqSinkOptions.PARTITION_KEY_FIELDS).isPresent()) {
            producerMetadata.setPartitionKeyFields(
                    pluginConfig.get(RocketMqSinkOptions.PARTITION_KEY_FIELDS));
        }
        producerMetadata.setExactlyOnce(pluginConfig.get(RocketMqSinkOptions.EXACTLY_ONCE));
        producerMetadata.setSync(pluginConfig.get(RocketMqSinkOptions.SEND_SYNC));
    }

    @Override
    public String getPluginName() {
        return RocketMqSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new RocketMqSinkWriter(producerMetadata, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
