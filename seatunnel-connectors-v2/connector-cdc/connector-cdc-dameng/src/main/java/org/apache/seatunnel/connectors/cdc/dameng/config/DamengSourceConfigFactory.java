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

package org.apache.seatunnel.connectors.cdc.dameng.config;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import java.util.Properties;
import java.util.UUID;

public class DamengSourceConfigFactory extends JdbcSourceConfigFactory {
    private static final String DRIVER_CLASS_NAME = "dm.jdbc.driver.DmDriver";

    @Override
    public DamengSourceConfig create(int subtaskId) {
        Properties props = new Properties();

        props.setProperty("database.server.name", "dameng_logminer_source");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        //TODO Not yet supported
        props.setProperty("include.schema.changes", String.valueOf(false));

        if (databaseList != null) {
            props.setProperty("schema.include.list", String.join(",", databaseList));
        }
        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }
        if (serverTimeZone != null) {
            props.setProperty("database.serverTimezone", serverTimeZone);
        }

        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        return new DamengSourceConfig(
            startupConfig,
            stopConfig,
            databaseList,
            tableList,
            splitSize,
            distributionFactorUpper,
            distributionFactorLower,
            props,
            DRIVER_CLASS_NAME,
            hostname,
            port,
            username,
            password,
            fetchSize,
            serverTimeZone,
            connectTimeoutMillis,
            connectMaxRetries,
            connectionPoolSize);
    }
}
