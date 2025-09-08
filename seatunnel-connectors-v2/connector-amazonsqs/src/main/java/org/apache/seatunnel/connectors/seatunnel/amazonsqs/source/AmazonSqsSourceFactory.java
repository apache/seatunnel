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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.MessageFormat;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.DEBEZIUM_RECORD_INCLUDE_SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.DELETE_MESSAGE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.MESSAGE_GROUP_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.SECRET_ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions.URL;

@AutoService(Factory.class)
public class AmazonSqsSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "AmazonSqs";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, REGION, SCHEMA)
                .optional(
                        ACCESS_KEY_ID,
                        SECRET_ACCESS_KEY,
                        MESSAGE_GROUP_ID,
                        DELETE_MESSAGE,
                        FORMAT,
                        FIELD_DELIMITER,
                        DEBEZIUM_RECORD_INCLUDE_SCHEMA)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(context.getOptions());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                setDeserialization(context.getOptions().toConfig(), catalogTable);
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new AmazonSqsSource(
                                new AmazonSqsSourceConfig(context.getOptions()),
                                catalogTable,
                                deserializationSchema);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AmazonSqsSource.class;
    }

    private DeserializationSchema<SeaTunnelRow> setDeserialization(
            Config config, CatalogTable catalogTable) {
        DeserializationSchema<SeaTunnelRow> deserializationSchema;
        MessageFormat format = ReadonlyConfig.fromConfig(config).get(FORMAT);
        switch (format) {
            case JSON:
                deserializationSchema = new JsonDeserializationSchema(catalogTable, false, false);
                break;
            case TEXT:
                String delimiter = DEFAULT_FIELD_DELIMITER;
                if (config.hasPath(FIELD_DELIMITER.key())) {
                    delimiter = config.getString(FIELD_DELIMITER.key());
                }
                deserializationSchema =
                        TextDeserializationSchema.builder()
                                .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                                .delimiter(delimiter)
                                .build();
                break;
            case CANAL_JSON:
                deserializationSchema =
                        CanalJsonDeserializationSchema.builder(catalogTable)
                                .setIgnoreParseErrors(true)
                                .build();
                break;
            case DEBEZIUM_JSON:
                boolean includeSchema = DEBEZIUM_RECORD_INCLUDE_SCHEMA.defaultValue();
                if (config.hasPath(DEBEZIUM_RECORD_INCLUDE_SCHEMA.key())) {
                    includeSchema = config.getBoolean(DEBEZIUM_RECORD_INCLUDE_SCHEMA.key());
                }
                deserializationSchema =
                        new DebeziumJsonDeserializationSchema(catalogTable, true, includeSchema);
                break;
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unsupported format: " + format);
        }
        return deserializationSchema;
    }
}
