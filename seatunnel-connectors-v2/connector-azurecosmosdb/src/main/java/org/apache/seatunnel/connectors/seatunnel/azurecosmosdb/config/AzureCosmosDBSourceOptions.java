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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.io.Serializable;

public class AzureCosmosDBSourceOptions extends ConnectorCommonOptions implements Serializable {

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB account URI");

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB account endpoint");

    public static final Option<String> KEY =
            Options.key("key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB account key");

    public static final Option<String> PRIMARY_KEY =
            Options.key("primary_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB primary account key");

    public static final Option<String> SECONDARY_KEY =
            Options.key("secondary_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB secondary account key");

    public static final Option<String> PRIMARY_CONNECTION_STRING =
            Options.key("primary_connection_string")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB primary connection string");

    public static final Option<String> SECONDARY_CONNECTION_STRING =
            Options.key("secondary_connection_string")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB secondary connection string");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB database name");

    public static final Option<String> CONTAINER =
            Options.key("container")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Cosmos DB container name");

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .defaultValue("SELECT * FROM c")
                    .withDescription("Cosmos SQL query used to read source data");

    public static final Option<Integer> MAX_ITEM_COUNT =
            Options.key("max_item_count")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Max item count per Cosmos query page");
}
