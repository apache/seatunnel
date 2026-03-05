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

package org.apache.seatunnel.api.datasource.gravitino;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.datasource.DataSourceMapper;
import org.apache.seatunnel.api.metalake.gravitino.GravitinoClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper for converting Gravitino JDBC data source metadata to SeaTunnel JDBC connector
 * configuration.
 *
 * <p>Gravitino response example:
 *
 * <pre>
 * {
 *   "code": 0,
 *   "catalog": {
 *     "name": "local-mysql",
 *     "type": "relational",
 *     "provider": "jdbc-mysql",
 *     "properties": {
 *       "jdbc-url": "jdbc:mysql://localhost:3306/",
 *       "jdbc-user": "root",
 *       "jdbc-driver": "com.mysql.cj.jdbc.Driver",
 *       "jdbc-password": "123456"
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>Maps to SeaTunnel JDBC config:
 *
 * <pre>
 * {
 *   "url": "jdbc:mysql://localhost:3306/",
 *   "username": "root",
 *   "password": "123456",
 *   "driver": "com.mysql.cj.jdbc.Driver"
 * }
 * </pre>
 */
public class GravitinoJdbcDataSourceMapper implements DataSourceMapper {

    private static final String GRAVITINO_JDBC_URL = "jdbc-url";
    private static final String GRAVITINO_JDBC_USER = "jdbc-user";
    private static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
    private static final String GRAVITINO_JDBC_DRIVER = "jdbc-driver";

    private static final String SEATUNNEL_URL = "url";
    private static final String SEATUNNEL_USERNAME = "username";
    private static final String SEATUNNEL_PASSWORD = "password";
    private static final String SEATUNNEL_DRIVER = "driver";

    private final String catalogBaseUrl;
    private final GravitinoClient client;

    public GravitinoJdbcDataSourceMapper(String catalogBaseUrl, GravitinoClient client) {
        this.catalogBaseUrl = catalogBaseUrl;
        this.client = client;
    }

    @Override
    public String connectorIdentifier() {
        return "Jdbc";
    }

    @Override
    public Map<String, Object> map(String datasourceId) {
        try {
            JsonNode propertiesNode = client.getMetaInfo(datasourceId, catalogBaseUrl);
            return convertToJdbcConfig(propertiesNode);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to fetch metadata from Gravitino for datasource: %s",
                            datasourceId),
                    e);
        }
    }

    /**
     * Converts Gravitino properties to SeaTunnel JDBC connector configuration.
     *
     * <p>Mapping:
     *
     * <ul>
     *   <li>jdbc-url → url
     *   <li>jdbc-user → username
     *   <li>jdbc-password → password
     *   <li>jdbc-driver → driver
     * </ul>
     *
     * @param propertiesNode Gravitino properties JSON node
     * @return SeaTunnel JDBC configuration map
     */
    private Map<String, Object> convertToJdbcConfig(JsonNode propertiesNode) {
        Map<String, Object> config = new HashMap<>();
        if (propertiesNode.has(GRAVITINO_JDBC_URL)) {
            config.put(SEATUNNEL_URL, propertiesNode.get(GRAVITINO_JDBC_URL).asText());
        }
        if (propertiesNode.has(GRAVITINO_JDBC_USER)) {
            config.put(SEATUNNEL_USERNAME, propertiesNode.get(GRAVITINO_JDBC_USER).asText());
        }
        if (propertiesNode.has(GRAVITINO_JDBC_PASSWORD)) {
            config.put(SEATUNNEL_PASSWORD, propertiesNode.get(GRAVITINO_JDBC_PASSWORD).asText());
        }
        if (propertiesNode.has(GRAVITINO_JDBC_DRIVER)) {
            config.put(SEATUNNEL_DRIVER, propertiesNode.get(GRAVITINO_JDBC_DRIVER).asText());
        }
        return config;
    }
}
