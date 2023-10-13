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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.DEBEZIUM_RECORD_INCLUDE_SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.URL;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class AmazonSqsSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private AmazonSqsSourceOptions amazonSqsSourceOptions;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private SeaTunnelRowType typeInfo;

    @Override
    public String getPluginName() {
        return "amazonsqs";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, URL.key(), REGION.key(), TableSchemaOptions.SCHEMA.key());
        if (!result.isSuccess()) {
            throw new AmazonSqsConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        amazonSqsSourceOptions = new AmazonSqsSourceOptions(pluginConfig);
        typeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();

        setDeserialization(pluginConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.typeInfo;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new AmazonSqsSourceReader(
                readerContext, amazonSqsSourceOptions, deserializationSchema, typeInfo);
    }

    private void setDeserialization(Config config) {
        if (config.hasPath(TableSchemaOptions.SCHEMA.key())) {
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
