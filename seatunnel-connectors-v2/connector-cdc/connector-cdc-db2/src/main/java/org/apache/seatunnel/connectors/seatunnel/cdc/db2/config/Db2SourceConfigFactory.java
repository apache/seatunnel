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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import io.debezium.connector.db2.Db2Connector;

import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** Factory for creating {@link Db2SourceConfig}. */
public class Db2SourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private static final String DATABASE_SERVER_NAME = "db2_transaction_log_source";
    private static final String DRIVER_CLASS_NAME = "com.ibm.db2.jcc.DB2Driver";

    @Override
    public Db2SourceConfig create(int subtask) {
        Properties props = new Properties();
        props.setProperty("connector.class", Db2Connector.class.getCanonicalName());

        // Debezium uses the logical server name as a namespace for offsets and schema history.
        // SeaTunnel isolates each source task with a generated history instance name below, so a
        // stable connector-level logical name is enough here.
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));

        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        // TODO Not yet supported
        props.setProperty("include.schema.changes", String.valueOf(false));

        // Db2 captures a single configured database via database.dbname. Debezium reports captured
        // table ids without a catalog, so setting database.include.list would filter out valid
        // Db2 change tables before snapshot split discovery.
        if (tableList != null) {
            // SeaTunnel table names are database.schema.table, while Debezium Db2 expects
            // schema.table in table.include.list for the single configured database.
            String tableIncludeList =
                    tableList.stream()
                            .map(table -> table.substring(table.indexOf(".") + 1))
                            .collect(Collectors.joining(","));
            props.setProperty("table.include.list", tableIncludeList);
        }

        if (dbzProperties != null) {
            dbzProperties.forEach(props::put);
        }

        return new Db2SourceConfig(
                startupConfig,
                stopConfig,
                databaseList,
                tableList,
                splitSize,
                splitColumn,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                inverseSamplingRate,
                sampleShardingAllow,
                props,
                DRIVER_CLASS_NAME,
                hostname,
                port,
                username,
                password,
                originUrl,
                fetchSize,
                serverTimeZone,
                connectTimeoutMillis,
                connectMaxRetries,
                connectionPoolSize,
                exactlyOnce);
    }
}
