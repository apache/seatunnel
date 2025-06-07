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

package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkQueryInfo;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class Neo4jSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return Neo4jSinkOptions.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Neo4jSinkOptions.KEY_NEO4J_URI,
                        Neo4jSinkOptions.KEY_DATABASE,
                        Neo4jSinkOptions.KEY_QUERY,
                        Neo4jSinkOptions.QUERY_PARAM_POSITION)
                .optional(
                        Neo4jSinkOptions.KEY_USERNAME,
                        Neo4jSinkOptions.KEY_PASSWORD,
                        Neo4jSinkOptions.KEY_BEARER_TOKEN,
                        Neo4jSinkOptions.KEY_KERBEROS_TICKET,
                        Neo4jSinkOptions.KEY_MAX_CONNECTION_TIMEOUT,
                        Neo4jSinkOptions.KEY_MAX_TRANSACTION_RETRY_TIME)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        Neo4jSinkQueryInfo neo4jSinkQueryInfo =
                new Neo4jSinkQueryInfo(context.getOptions().toConfig());
        return () -> new Neo4jSink(context.getCatalogTable(), neo4jSinkQueryInfo);
    }
}
