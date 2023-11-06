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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;
import org.apache.seatunnel.format.compatible.kafka.connect.json.CompatibleKafkaConnectDeserializationSchema;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.RawDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import org.apache.kafka.common.TopicPartition;

import com.google.auto.service.AutoService;

import com.google.common.collect.Lists;

import java.util.List;

public class KafkaSource
        implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState>,
                SupportParallelism {

    private JobContext jobContext;

    private final KafkaSourceConfig kafkaSourceConfig;

    public KafkaSource(ReadonlyConfig readonlyConfig) {
        kafkaSourceConfig = new KafkaSourceConfig(readonlyConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONNECTOR_IDENTITY;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(kafkaSourceConfig.getCatalogTable());
    }

    @Override
    public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new KafkaSourceReader(
                kafkaSourceConfig.getMetadata(),
                kafkaSourceConfig.getDeserializationSchema(),
                readerContext,
                kafkaSourceConfig.getMessageFormatErrorHandleWay());
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> createEnumerator(
            SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext) {
        return new KafkaSourceSplitEnumerator(
                kafkaSourceConfig.getMetadata(),
                enumeratorContext,
                kafkaSourceConfig.getDiscoveryIntervalMillis());
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext,
            KafkaSourceState checkpointState) {
        return new KafkaSourceSplitEnumerator(
                kafkaSourceConfig.getMetadata(),
                enumeratorContext,
                checkpointState,
                kafkaSourceConfig.getDiscoveryIntervalMillis());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(Config config) {
        if (config.hasPath(SCHEMA.key())) {
            Config schema = config.getConfig(SCHEMA.key());
            // todo: use KafkaDataTypeConvertor here?
            typeInfo = CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();
            MessageFormat format = ReadonlyConfig.fromConfig(config).get(FORMAT);
            switch (format) {
                case JSON:
                    deserializationSchema = new JsonDeserializationSchema(false, false, typeInfo);
                    break;
                case TEXT:
                    String delimiter = DEFAULT_FIELD_DELIMITER;
                    if (config.hasPath(FIELD_DELIMITER.key())) {
                        delimiter = config.getString(FIELD_DELIMITER.key());
                    }
                    deserializationSchema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(typeInfo)
                                    .delimiter(delimiter)
                                    .build();
                    break;
                case CANAL_JSON:
                    deserializationSchema =
                            CanalJsonDeserializationSchema.builder(typeInfo)
                                    .setIgnoreParseErrors(true)
                                    .build();
                    break;
                case RAW:
                    deserializationSchema =
                            RawDeserializationSchema.builder().seaTunnelRowType(typeInfo).build();
                    break;
                case COMPATIBLE_KAFKA_CONNECT_JSON:
                    deserializationSchema =
                            new CompatibleKafkaConnectDeserializationSchema(
                                    typeInfo, config, false, false);
                    break;
                case DEBEZIUM_JSON:
                    boolean includeSchema = DEBEZIUM_RECORD_INCLUDE_SCHEMA.defaultValue();
                    if (config.hasPath(DEBEZIUM_RECORD_INCLUDE_SCHEMA.key())) {
                        includeSchema = config.getBoolean(DEBEZIUM_RECORD_INCLUDE_SCHEMA.key());
                    }
                    deserializationSchema =
                            new DebeziumJsonDeserializationSchema(typeInfo, true, includeSchema);
                    break;
                default:
                    throw new SeaTunnelJsonFormatException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
            }
        } else {
            typeInfo = CatalogTableUtil.buildSimpleTextSchema();
            this.deserializationSchema =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(typeInfo)
                            .delimiter(TextFormatConstant.PLACEHOLDER)
                            .build();
        }
    }
}
