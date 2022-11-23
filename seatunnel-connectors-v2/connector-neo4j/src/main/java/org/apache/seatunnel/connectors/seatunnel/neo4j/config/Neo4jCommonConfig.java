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

package org.apache.seatunnel.connectors.seatunnel.neo4j.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public abstract class Neo4jCommonConfig {

    public static final String PLUGIN_NAME = "Neo4j";
    public static final Long DEFAULT_MAX_TRANSACTION_RETRY_TIME = 30L;
    public static final Long DEFAULT_MAX_CONNECTION_TIMEOUT = 30L;

    public static final Option<String> KEY_NEO4J_URI =
        Options.key("uri")
            .stringType()
            .noDefaultValue()
            .withDescription("The URI of the Neo4j database");

    public static final Option<String> KEY_USERNAME =
        Options.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("username of the Neo4j");

    public static final Option<String> KEY_PASSWORD =
        Options.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("password of the Neo4j");

    public static final Option<String> KEY_BEARER_TOKEN =
        Options.key("bearer_token")
            .stringType()
            .noDefaultValue()
            .withDescription("base64 encoded bearer token of the Neo4j. for Auth.");

    public static final Option<String> KEY_KERBEROS_TICKET =
        Options.key("kerberos_ticket")
            .stringType()
            .noDefaultValue()
            .withDescription("base64 encoded kerberos ticket of the Neo4j. for Auth.");

    public static final Option<String> KEY_DATABASE =
        Options.key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("database name.");

    public static final Option<String> KEY_QUERY =
        Options.key("query")
            .stringType()
            .noDefaultValue()
            .withDescription("Query statement.");

    public static final Option<Long> KEY_MAX_TRANSACTION_RETRY_TIME =
        Options.key("max_transaction_retry_time")
            .longType()
            .defaultValue(DEFAULT_MAX_TRANSACTION_RETRY_TIME)
            .withDescription("maximum transaction retry time(seconds). transaction fail if exceeded.");

    public static final Option<Long> KEY_MAX_CONNECTION_TIMEOUT =
        Options.key("max_connection_timeout")
            .longType()
            .defaultValue(DEFAULT_MAX_CONNECTION_TIMEOUT)
            .withDescription("The maximum amount of time to wait for a TCP connection to be established (seconds).");
}
