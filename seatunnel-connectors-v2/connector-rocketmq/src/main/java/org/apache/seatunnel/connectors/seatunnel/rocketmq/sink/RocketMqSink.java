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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.config.ProducerConfig;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.ACL_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.CONNECTOR_IDENTITY;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.NAME_SRV_ADDR;
import static org.apache.seatunnel.connectors.seatunnel.rocketmq.config.Config.SECRET_KEY;

@AutoService(SeaTunnelSink.class)
public class RocketMqSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private static final String DEFAULT_PRODUCER_GROUP = "SeaTunnel-Producer-Group";
    private SeaTunnelRowType seaTunnelRowType;
    private ProducerMetadata producerMetadata;

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        config, ProducerConfig.TOPIC.key(), NAME_SRV_ADDR.key());
        if (!result.isSuccess()) {
            throw new RocketMqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        producerMetadata = new ProducerMetadata();
        producerMetadata.setTopic(config.getString(ProducerConfig.TOPIC.key()));
        if (config.hasPath(ProducerConfig.TAG.key())) {
            producerMetadata.setTag(config.getString(ProducerConfig.TAG.key()));
        }
        RocketMqBaseConfiguration.Builder baseConfigurationBuilder =
                RocketMqBaseConfiguration.newBuilder()
                        .producer()
                        .namesrvAddr(config.getString(NAME_SRV_ADDR.key()));
        // acl config
        boolean aclEnabled = ACL_ENABLED.defaultValue();
        if (config.hasPath(ACL_ENABLED.key())) {
            aclEnabled = config.getBoolean(ACL_ENABLED.key());
            if (aclEnabled
                    && (!config.hasPath(ACCESS_KEY.key()) || !config.hasPath(SECRET_KEY.key()))) {
                throw new RocketMqConnectorException(
                        SeaTunnelAPIErrorCode.OPTION_VALIDATION_FAILED,
                        "When ACL_ENABLED "
                                + "true , ACCESS_KEY and SECRET_KEY must be configured");
            }
            if (config.hasPath(ACCESS_KEY.key())) {
                baseConfigurationBuilder.accessKey(config.getString(ACCESS_KEY.key()));
            }
            if (config.hasPath(SECRET_KEY.key())) {
                baseConfigurationBuilder.secretKey(config.getString(SECRET_KEY.key()));
            }
        }
        baseConfigurationBuilder.aclEnable(aclEnabled);

        // config producer group
        if (config.hasPath(ProducerConfig.PRODUCER_GROUP.key())) {
            baseConfigurationBuilder.groupId(config.getString(ProducerConfig.PRODUCER_GROUP.key()));
        } else {
            baseConfigurationBuilder.groupId(DEFAULT_PRODUCER_GROUP);
        }

        if (config.hasPath(ProducerConfig.MAX_MESSAGE_SIZE.key())) {
            baseConfigurationBuilder.maxMessageSize(
                    config.getInt(ProducerConfig.MAX_MESSAGE_SIZE.key()));
        } else {
            baseConfigurationBuilder.maxMessageSize(ProducerConfig.MAX_MESSAGE_SIZE.defaultValue());
        }

        if (config.hasPath(ProducerConfig.SEND_MESSAGE_TIMEOUT_MILLIS.key())) {
            baseConfigurationBuilder.sendMsgTimeout(
                    config.getInt(ProducerConfig.SEND_MESSAGE_TIMEOUT_MILLIS.key()));
        } else {
            baseConfigurationBuilder.sendMsgTimeout(
                    ProducerConfig.SEND_MESSAGE_TIMEOUT_MILLIS.defaultValue());
        }

        this.producerMetadata.setConfiguration(baseConfigurationBuilder.build());

        if (config.hasPath(FORMAT.key())) {
            producerMetadata.setFormat(
                    SchemaFormat.valueOf(config.getString(FORMAT.key()).toUpperCase()));
        } else {
            producerMetadata.setFormat(SchemaFormat.JSON);
        }

        if (config.hasPath(FIELD_DELIMITER.key())) {
            producerMetadata.setFieldDelimiter(config.getString(FIELD_DELIMITER.key()));
        } else {
            producerMetadata.setFieldDelimiter(DEFAULT_FIELD_DELIMITER);
        }

        if (config.hasPath(ProducerConfig.PARTITION_KEY_FIELDS.key())) {
            producerMetadata.setPartitionKeyFields(
                    config.getStringList(ProducerConfig.PARTITION_KEY_FIELDS.key()));
        }

        boolean exactlyOnce = ProducerConfig.EXACTLY_ONCE.defaultValue();
        if (config.hasPath(ProducerConfig.EXACTLY_ONCE.key())) {
            exactlyOnce = config.getBoolean(ProducerConfig.EXACTLY_ONCE.key());
        }
        producerMetadata.setExactlyOnce(exactlyOnce);

        boolean sync = ProducerConfig.SEND_SYNC.defaultValue();
        if (config.hasPath(ProducerConfig.SEND_SYNC.key())) {
            sync = config.getBoolean(ProducerConfig.SEND_SYNC.key());
        }
        producerMetadata.setSync(sync);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new RocketMqSinkWriter(producerMetadata, seaTunnelRowType);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return super.getWriteCatalogTable();
    }
}
