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
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_BEARER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_KERBEROS_TICKET;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_MAX_CONNECTION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_MAX_TRANSACTION_RETRY_TIME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_NEO4J_URI;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.KEY_USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.PLUGIN_NAME;

@AutoService(Factory.class)
public class Neo4jSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KEY_NEO4J_URI, KEY_DATABASE, KEY_QUERY, ConnectorCommonOptions.SCHEMA)
                .optional(
                        KEY_USERNAME,
                        KEY_PASSWORD,
                        KEY_BEARER_TOKEN,
                        KEY_KERBEROS_TICKET,
                        KEY_MAX_CONNECTION_TIMEOUT,
                        KEY_MAX_TRANSACTION_RETRY_TIME)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return Neo4jSource.class;
    }
}
