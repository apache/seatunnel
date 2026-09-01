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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.CONTAINER;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.KEY;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.MAX_ITEM_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.PRIMARY_CONNECTION_STRING;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.SECONDARY_CONNECTION_STRING;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.SECONDARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSourceOptions.URI;

@AutoService(Factory.class)
public class AzureCosmosDBSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "AzureCosmosDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(DATABASE, CONTAINER, SCHEMA)
                .optional(
                        URI,
                        ENDPOINT,
                        KEY,
                        PRIMARY_KEY,
                        SECONDARY_KEY,
                        PRIMARY_CONNECTION_STRING,
                        SECONDARY_CONNECTION_STRING,
                        QUERY,
                        MAX_ITEM_COUNT)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new AzureCosmosDBSource(
                                new AzureCosmosDBConfig(context.getOptions()),
                                CatalogTableUtil.buildWithConfig(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AzureCosmosDBSource.class;
    }
}
