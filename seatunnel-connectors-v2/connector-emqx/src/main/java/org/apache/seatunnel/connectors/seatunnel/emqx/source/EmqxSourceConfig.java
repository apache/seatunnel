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

package org.apache.seatunnel.connectors.seatunnel.emqx.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.emqx.config.Config;
import org.apache.seatunnel.connectors.seatunnel.emqx.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.connectors.seatunnel.emqx.config.PayloadFormat;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
public class EmqxSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ClientMetadata metadata;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private final List<CatalogTable> catalogTables;

    private final MessageFormatErrorHandleWay messageFormatErrorHandleWay;

    public EmqxSourceConfig(ReadonlyConfig readonlyConfig) {
        this.metadata = createConsumerMetadata(readonlyConfig);
        this.messageFormatErrorHandleWay =
                readonlyConfig.get(Config.MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION);
        this.catalogTables = createCatalogTable(readonlyConfig);
        this.deserializationSchema = createDeserializationSchema(catalogTables, readonlyConfig);
    }

    private ClientMetadata createConsumerMetadata(ReadonlyConfig readonlyConfig) {
        ClientMetadata clientMetadata = new ClientMetadata();
        clientMetadata.setTopic(readonlyConfig.get(Config.TOPIC));
        clientMetadata.setBroker(readonlyConfig.get(Config.BROKER));
        clientMetadata.setClientId(readonlyConfig.get(Config.CLIENT_ID));
        clientMetadata.setUsername(readonlyConfig.get(Config.USERNAME));
        clientMetadata.setPassword(readonlyConfig.get(Config.PASSWORD));
        clientMetadata.setQos(readonlyConfig.get(Config.QOS));
        clientMetadata.setCleanSession(readonlyConfig.get(Config.CLEAN_SESSION));
        return clientMetadata;
    }

    private List<CatalogTable> createCatalogTable(ReadonlyConfig readonlyConfig) {
        Optional<Map<String, Object>> schemaOptions =
                readonlyConfig.getOptional(TableSchemaOptions.SCHEMA);
        if (schemaOptions.isPresent()) {
            return Lists.newArrayList(CatalogTableUtil.buildWithConfig(readonlyConfig));
        } else {
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(Config.CONNECTOR_IDENTITY, null, null);
            TableSchema tableSchema =
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "content",
                                            new SeaTunnelRowType(
                                                    new String[] {"content"},
                                                    new SeaTunnelDataType<?>[] {
                                                        BasicType.STRING_TYPE
                                                    }),
                                            0L,
                                            false,
                                            null,
                                            null,
                                            null,
                                            null))
                            .build();
            return Lists.newArrayList(
                    CatalogTable.of(
                            tableIdentifier,
                            tableSchema,
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            null));
        }
    }

    private DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            List<CatalogTable> catalogTables, ReadonlyConfig readonlyConfig) {
        SeaTunnelRowType seaTunnelRowType = catalogTables.get(0).getSeaTunnelRowType();
        PayloadFormat format = readonlyConfig.get(Config.FORMAT);
        if (!readonlyConfig.getOptional(TableSchemaOptions.SCHEMA).isPresent()) {
            return TextDeserializationSchema.builder()
                    .seaTunnelRowType(seaTunnelRowType)
                    .delimiter(TextFormatConstant.PLACEHOLDER)
                    .build();
        }
        switch (format) {
            case JSON:
                return new JsonDeserializationSchema(false, false, seaTunnelRowType);
            case TEXT:
                String delimiter = readonlyConfig.get(Config.FIELD_DELIMITER);
                return TextDeserializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .delimiter(delimiter)
                        .build();
            default:
                throw new SeaTunnelRuntimeException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Unsupported format: " + format);
        }
    }
}
