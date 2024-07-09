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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.sql.Connection;
import java.sql.SQLException;

import static io.debezium.connector.mysql.MySqlConnectorConfig.JDBC_DRIVER;

public class CustomMySqlConnectionConfiguration
        extends MySqlConnection.MySqlConnectionConfiguration {

    protected static final String URL_PATTERN =
            "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";

    private final JdbcConnection.ConnectionFactory connectionFactory;

    public CustomMySqlConnectionConfiguration(Configuration config) {
        super(config);
        String driverClassName =
                config.getString(JDBC_DRIVER.name(), JDBC_DRIVER.defaultValueAsString());
        connectionFactory =
                JdbcConnection.patternBasedFactory(
                        URL_PATTERN, driverClassName, getClass().getClassLoader());
    }

    @Override
    public JdbcConnection.ConnectionFactory factory() {
        return new JdbcConnection.ConnectionFactory() {
            @Override
            public Connection connect(JdbcConfiguration config) throws SQLException {
                return connectionFactory.connect(config);
            }
        };
    }
}
