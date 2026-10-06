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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.neo4j.driver.AuthTokens;

import lombok.Data;

import java.io.Serializable;
import java.net.URI;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_BEARER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_KERBEROS_TICKET;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_MAX_CONNECTION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_MAX_TRANSACTION_RETRY_TIME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_NEO4J_URI;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jBaseOptions.KEY_USERNAME;

/**
 * Because Neo4jQueryInfo is one of the Neo4jSink's member variable, So Neo4jQueryInfo need
 * implements Serializable interface
 */
@Data
public abstract class Neo4jQueryInfo implements Serializable {
    protected DriverBuilder driverBuilder;
    protected String query;

    public Neo4jQueryInfo(Config config) {
        this.driverBuilder = prepareDriver(config);
        this.query = prepareQuery(config);
    }

    protected DriverBuilder prepareDriver(Config config) {
        final URI uri = URI.create(config.getString(KEY_NEO4J_URI.key()));

        final DriverBuilder driverBuilder = DriverBuilder.create(uri);

        if (config.hasPath(KEY_USERNAME.key())) {
            final String username = config.getString(KEY_USERNAME.key());
            final String password = config.getString(KEY_PASSWORD.key());

            driverBuilder.setUsername(username);
            driverBuilder.setPassword(password);
        } else if (config.hasPath(KEY_BEARER_TOKEN.key())) {
            final String bearerToken = config.getString(KEY_BEARER_TOKEN.key());
            AuthTokens.bearer(bearerToken);
            driverBuilder.setBearerToken(bearerToken);
        } else {
            final String kerberosTicket = config.getString(KEY_KERBEROS_TICKET.key());
            AuthTokens.kerberos(kerberosTicket);
            driverBuilder.setBearerToken(kerberosTicket);
        }

        driverBuilder.setDatabase(config.getString(KEY_DATABASE.key()));

        if (config.hasPath(KEY_MAX_CONNECTION_TIMEOUT.key())) {
            driverBuilder.setMaxConnectionTimeoutSeconds(
                    config.getLong(KEY_MAX_CONNECTION_TIMEOUT.key()));
        }
        if (config.hasPath(KEY_MAX_TRANSACTION_RETRY_TIME.key())) {
            driverBuilder.setMaxTransactionRetryTimeSeconds(
                    config.getLong(KEY_MAX_TRANSACTION_RETRY_TIME.key()));
        }

        return driverBuilder;
    }

    private String prepareQuery(Config config) {
        return config.getString(KEY_QUERY.key());
    }
}
