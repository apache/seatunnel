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

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
public class Neo4jSourceConfig implements Serializable {

    public static final String PLUGIN_NAME = "Neo4j";

    public static final String KEY_NEO4J_URI = "uri";
    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_BEARER_TOKEN = "bearer_token";
    public static final String KEY_KERBEROS_TICKET = "kerberos_ticket"; // Base64 encoded

    public static final String KEY_DATABASE = "database";
    public static final String KEY_QUERY = "query";
    public static final String KEY_MAX_TRANSACTION_RETRY_TIME = "max_transaction_retry_time";
    public static final String KEY_MAX_CONNECTION_TIMEOUT = "max_connection_timeout";


    private DriverBuilder driverBuilder;
    private String query;
}
