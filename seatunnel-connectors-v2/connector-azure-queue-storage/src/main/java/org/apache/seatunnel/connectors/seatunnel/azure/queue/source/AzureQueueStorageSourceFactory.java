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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.source;

import org.apache.seatunnel.api.configuration.util.Conditions;
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
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AuthenticationType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueStorageSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageFormat;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;

@AutoService(Factory.class)
public class AzureQueueStorageSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return AzureQueueStorageSource.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AzureQueueStorageSourceOptions.QUEUE_NAME,
                        AzureQueueStorageSourceOptions.AUTHENTICATION_TYPE,
                        SCHEMA)
                .conditional(
                        AzureQueueStorageSourceOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.CONNECTION_STRING,
                        AzureQueueStorageSourceOptions.CONNECTION_STRING)
                .conditional(
                        AzureQueueStorageSourceOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.SHARED_KEY,
                        AzureQueueStorageSourceOptions.ENDPOINT,
                        AzureQueueStorageSourceOptions.ACCOUNT_NAME,
                        AzureQueueStorageSourceOptions.ACCOUNT_KEY)
                .conditional(
                        AzureQueueStorageSourceOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.SAS_TOKEN,
                        AzureQueueStorageSourceOptions.ENDPOINT,
                        AzureQueueStorageSourceOptions.SAS_TOKEN)
                .optional(
                        AzureQueueStorageSourceOptions.FORMAT,
                        AzureQueueStorageSourceOptions.FIELD_DELIMITER,
                        AzureQueueStorageSourceOptions.MESSAGE_ENCODING)
                .optional(
                        AzureQueueStorageSourceOptions.BATCH_SIZE,
                        Conditions.greaterOrEqual(AzureQueueStorageSourceOptions.BATCH_SIZE, 1)
                                .and(
                                        Conditions.lessOrEqual(
                                                AzureQueueStorageSourceOptions.BATCH_SIZE, 32)))
                .optional(
                        AzureQueueStorageSourceOptions.VISIBILITY_TIMEOUT_SECONDS,
                        Conditions.greaterOrEqual(
                                        AzureQueueStorageSourceOptions.VISIBILITY_TIMEOUT_SECONDS,
                                        1)
                                .and(
                                        Conditions.lessOrEqual(
                                                AzureQueueStorageSourceOptions
                                                        .VISIBILITY_TIMEOUT_SECONDS,
                                                604_800)))
                .optional(
                        AzureQueueStorageSourceOptions.POLL_INTERVAL_MS,
                        Conditions.greaterThan(AzureQueueStorageSourceOptions.POLL_INTERVAL_MS, 0L))
                .optional(
                        AzureQueueStorageSourceOptions.MAX_IN_FLIGHT_MESSAGES,
                        Conditions.greaterThan(
                                AzureQueueStorageSourceOptions.MAX_IN_FLIGHT_MESSAGES, 0))
                .optional(
                        AzureQueueStorageSourceOptions.OPERATION_TIMEOUT_MS,
                        Conditions.greaterThan(
                                AzureQueueStorageSourceOptions.OPERATION_TIMEOUT_MS, 0L))
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AzureQueueSourceConfig config = AzureQueueSourceConfig.from(context.getOptions());
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(context.getOptions());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                createDeserializationSchema(catalogTable, config);

        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new AzureQueueStorageSource(config, catalogTable, deserializationSchema);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AzureQueueStorageSource.class;
    }

    private DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            CatalogTable catalogTable, AzureQueueSourceConfig config) {
        if (config.getFormat() == MessageFormat.JSON) {
            return new JsonDeserializationSchema(catalogTable, false, false);
        }
        return TextDeserializationSchema.builder()
                .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                .delimiter(config.getFieldDelimiter())
                .build();
    }
}
