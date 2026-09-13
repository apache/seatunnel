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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.source;

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
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.MessageFormat;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;

/** Creates Google Pub/Sub sources and their payload deserializers. */
@AutoService(Factory.class)
public class GooglePubSubSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return GooglePubSubSource.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        GooglePubSubSourceOptions.PROJECT_ID,
                        GooglePubSubSourceOptions.SUBSCRIPTION,
                        SCHEMA)
                .optional(
                        GooglePubSubSourceOptions.CREDENTIALS_PATH,
                        GooglePubSubSourceOptions.EMULATOR_HOST,
                        GooglePubSubSourceOptions.FORMAT,
                        GooglePubSubSourceOptions.FIELD_DELIMITER,
                        GooglePubSubSourceOptions.MAX_OUTSTANDING_MESSAGES,
                        GooglePubSubSourceOptions.MAX_OUTSTANDING_BYTES,
                        GooglePubSubSourceOptions.PARALLEL_PULL_COUNT)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        GooglePubSubSourceConfig config = GooglePubSubSourceConfig.from(context.getOptions());
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(context.getOptions());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                createDeserializationSchema(catalogTable, config);

        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new GooglePubSubSource(config, catalogTable, deserializationSchema);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return GooglePubSubSource.class;
    }

    private DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            CatalogTable catalogTable, GooglePubSubSourceConfig config) {
        if (config.getFormat() == MessageFormat.JSON) {
            return new JsonDeserializationSchema(catalogTable, false, false);
        }
        return TextDeserializationSchema.builder()
                .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                .delimiter(config.getFieldDelimiter())
                .build();
    }
}
