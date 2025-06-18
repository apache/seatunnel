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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceQueryInfo;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class Neo4jSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return Neo4jSourceOptions.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Neo4jSourceOptions.KEY_NEO4J_URI,
                        Neo4jSourceOptions.KEY_DATABASE,
                        Neo4jSourceOptions.KEY_QUERY,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        Neo4jSourceOptions.KEY_USERNAME,
                        Neo4jSourceOptions.KEY_PASSWORD,
                        Neo4jSourceOptions.KEY_BEARER_TOKEN,
                        Neo4jSourceOptions.KEY_KERBEROS_TICKET,
                        Neo4jSourceOptions.KEY_MAX_CONNECTION_TIMEOUT,
                        Neo4jSourceOptions.KEY_MAX_TRANSACTION_RETRY_TIME)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return Neo4jSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        Neo4jSourceQueryInfo neo4jSourceQueryInfo =
                new Neo4jSourceQueryInfo(context.getOptions().toConfig());
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new Neo4jSource(
                                CatalogTableUtil.buildWithConfig(context.getOptions()),
                                neo4jSourceQueryInfo);
    }
}
