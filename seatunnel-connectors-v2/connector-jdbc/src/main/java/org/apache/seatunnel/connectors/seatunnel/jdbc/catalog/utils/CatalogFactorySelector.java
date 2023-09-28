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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils;

import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalogFactory;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class CatalogFactorySelector {
    private static final String PROTOCOL_MYSQL = "jdbc:mysql:";
    private static final String PROTOCOL_ORACLE = "jdbc:oracle:thin:";
    private static final String PROTOCOL_SQLSERVER = "jdbc:sqlserver:";
    private static final String PROTOCOL_POSTGRESQL = "jdbc:postgresql:";
    private static final String PROTOCOL_DB2 = "jdbc:db2:";
    private static final String PROTOCOL_DM = "jdbc:dm:";
    private static final String PROTOCOL_GBASE = "jdbc:gbase:";
    private static final String PROTOCOL_GREENPLUM = "jdbc:pivotal:greenplum:";
    private static final String PROTOCOL_INFORMIX = "jdbc:informix-sqli";
    private static final String PROTOCOL_KINGBASE8 = "jdbc:kingbase8:";
    private static final String PROTOCOL_PHOENIX = "jdbc:phoenix:";
    private static final String PROTOCOL_REDSHIFT = "jdbc:redshift:";
    private static final String PROTOCOL_SAP = "jdbc:sap:";
    private static final String PROTOCOL_SNOWFLAKE = "jdbc:snowflake:";
    private static final String PROTOCOL_SQLITE = "jdbc:sqlite:";
    private static final String PROTOCOL_TABLESTORE = "jdbc:ots:https:";
    private static final String PROTOCOL_TERADATA = "jdbc:teradata:";
    private static final String PROTOCOL_VERTICA = "jdbc:vertica:";

    public static Optional<CatalogFactory> select(String jdbcURL) {
        if (jdbcURL.startsWith(PROTOCOL_MYSQL)) {
            return Optional.of(new MySqlCatalogFactory());
        }
        if (jdbcURL.startsWith(PROTOCOL_ORACLE)) {
            return Optional.of(new OracleCatalogFactory());
        }
        if (jdbcURL.startsWith(PROTOCOL_SQLSERVER)) {
            return Optional.of(new SqlServerCatalogFactory());
        }
        if (jdbcURL.startsWith(PROTOCOL_POSTGRESQL)) {
            return Optional.of(new PostgresCatalogFactory());
        }
        if (jdbcURL.startsWith(PROTOCOL_DM)) {
            return Optional.of(new DamengCatalogFactory());
        }

        log.warn("No catalog factory found for jdbc url: {}", jdbcURL);

        return Optional.empty();
    }
}
