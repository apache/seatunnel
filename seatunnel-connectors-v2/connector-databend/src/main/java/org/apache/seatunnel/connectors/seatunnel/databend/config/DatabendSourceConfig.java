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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.AUTO_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.JDBC_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.SSL;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSourceOptions.SQL;

@Setter
@Getter
@ToString
public class DatabendSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // common options
    private String url;
    private Boolean ssl;
    private String username;
    private String password;
    private String database;
    private String table;
    private Boolean autoCommit;
    private Integer maxRetries;
    private Map<String, String> jdbcConfig;

    // source options
    private String query;
    private String sql;
    private Integer fetchSize;
    private Properties properties;

    public static DatabendSourceConfig of(ReadonlyConfig config) {
        DatabendSourceConfig sourceConfig = new DatabendSourceConfig();

        // common options
        sourceConfig.setUrl(config.get(URL));
        sourceConfig.setSsl(config.get(SSL));
        sourceConfig.setUsername(config.get(USERNAME));
        sourceConfig.setPassword(config.get(PASSWORD));
        sourceConfig.setDatabase(config.get(DATABASE));
        sourceConfig.setTable(config.get(TABLE));
        sourceConfig.setAutoCommit(config.get(AUTO_COMMIT));
        sourceConfig.setMaxRetries(config.get(MAX_RETRIES));
        sourceConfig.setJdbcConfig(config.get(JDBC_CONFIG));

        // source options
        sourceConfig.setQuery(config.getOptional(QUERY).orElse(null));
        sourceConfig.setSql(config.getOptional(SQL).orElse(null));
        sourceConfig.setFetchSize(config.get(FETCH_SIZE));

        // Create properties for JDBC connection
        Properties properties = new Properties();
        if (sourceConfig.getJdbcConfig() != null) {
            sourceConfig.getJdbcConfig().forEach(properties::setProperty);
        }
        if (!properties.containsKey("user")) {
            properties.setProperty("user", sourceConfig.getUsername());
        }
        if (!properties.containsKey("password")) {
            properties.setProperty("password", sourceConfig.getPassword());
        }
        if (sourceConfig.getSsl() != null) {
            properties.setProperty("ssl", sourceConfig.getSsl().toString());
        }
        sourceConfig.setProperties(properties);
        return sourceConfig;
    }

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
        map.put(MAX_RETRIES.key(), maxRetries);
        if (jdbcConfig != null) {
            map.put(JDBC_CONFIG.key(), jdbcConfig);
        }
        if (query != null) {
            map.put(QUERY.key(), query);
        }
        if (sql != null) {
            map.put(SQL.key(), sql);
        }
        map.put(FETCH_SIZE.key(), fetchSize);

        return ReadonlyConfig.fromMap(map);
    }
}
