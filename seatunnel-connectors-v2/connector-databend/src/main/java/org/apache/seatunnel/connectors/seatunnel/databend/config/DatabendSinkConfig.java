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

package org.apache.seatunnel.connectors.seatunnel.databend.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.AUTO_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.JDBC_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.SSL;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkOptions.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkOptions.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkOptions.EXECUTE_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkOptions.SCHEMA_SAVE_MODE;

@Setter
@Getter
@ToString
public class DatabendSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // common options
    private String url;
    private String username;
    private String password;
    private Boolean ssl;
    private String database;
    private String table;
    private Boolean autoCommit;
    private Integer batchSize;
    private Integer maxRetries;
    private Map<String, String> jdbcConfig;

    // sink options
    private Integer executeTimeoutSec;
    private String customSql;
    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private Properties properties;

    public static DatabendSinkConfig of(ReadonlyConfig config) {
        DatabendSinkConfig sinkConfig = new DatabendSinkConfig();

        // common options
        sinkConfig.setUrl(config.get(URL));
        sinkConfig.setUsername(config.get(USERNAME));
        sinkConfig.setPassword(config.get(PASSWORD));
        sinkConfig.setDatabase(config.get(DATABASE));
        sinkConfig.setTable(config.get(TABLE));
        sinkConfig.setAutoCommit(config.get(AUTO_COMMIT));
        sinkConfig.setBatchSize(config.get(BATCH_SIZE));
        sinkConfig.setMaxRetries(config.get(MAX_RETRIES));
        sinkConfig.setJdbcConfig(config.get(JDBC_CONFIG));

        // sink options
        sinkConfig.setExecuteTimeoutSec(config.get(EXECUTE_TIMEOUT_SEC));
        sinkConfig.setCustomSql(config.getOptional(CUSTOM_SQL).orElse(null));
        sinkConfig.setSchemaSaveMode(config.get(SCHEMA_SAVE_MODE));
        sinkConfig.setDataSaveMode(config.get(DATA_SAVE_MODE));
        // Create properties for JDBC connection
        Properties properties = new Properties();
        if (sinkConfig.getJdbcConfig() != null) {
            sinkConfig.getJdbcConfig().forEach(properties::setProperty);
        }
        if (!properties.containsKey("user")) {
            properties.setProperty("user", sinkConfig.getUsername());
        }
        if (!properties.containsKey("password")) {
            properties.setProperty("password", sinkConfig.getPassword());
        }
        if (sinkConfig.getSsl() != null) {
            properties.setProperty("ssl", sinkConfig.getSsl().toString());
        }
        sinkConfig.setProperties(properties);

        return sinkConfig;
    }

    // Change UserName, password, jdbcConfig to properties from databendSinkConfig
    public Properties toProperties() {
        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        properties.put("ssl", ssl);
        if (jdbcConfig != null) {
            jdbcConfig.forEach(properties::put);
        }
        return properties;
    }
    /** Convert this config to a ReadonlyConfig */
    public ReadonlyConfig toReadonlyConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put(URL.key(), url);
        map.put(USERNAME.key(), username);
        map.put(PASSWORD.key(), password);
        if (ssl != null) {
            map.put(SSL.key(), ssl);
        }
        map.put(DATABASE.key(), database);
        map.put(TABLE.key(), table);
        map.put(AUTO_COMMIT.key(), autoCommit);
        map.put(BATCH_SIZE.key(), batchSize);
        map.put(MAX_RETRIES.key(), maxRetries);
        if (jdbcConfig != null) {
            map.put(JDBC_CONFIG.key(), jdbcConfig);
        }
        map.put(EXECUTE_TIMEOUT_SEC.key(), executeTimeoutSec);
        if (customSql != null) {
            map.put(CUSTOM_SQL.key(), customSql);
        }
        map.put(SCHEMA_SAVE_MODE.key(), schemaSaveMode);
        map.put(DATA_SAVE_MODE.key(), dataSaveMode);

        return ReadonlyConfig.fromMap(map);
    }
}
