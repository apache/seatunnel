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

package org.apache.seatunnel.api.metadata.gravitino;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.metadata.MetadataProvider;
import org.apache.seatunnel.api.metalake.gravitino.GravitinoClient;
import org.apache.seatunnel.api.metalake.gravitino.GravitinoTableSchemaConvertor;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Gravitino implementation of {@link MetadataProvider}.
 *
 * <p>This provider integrates with Apache Gravitino for centralized data source metadata
 * management.
 *
 * <p>Configuration (from seatunnel.yaml under seatunnel.engine.datasource):
 *
 * <pre>
 * metadata:
 *   enabled: true
 *   kind: gravitino
 *   uri: <a href="http://localhost:8090">...</a>          # Gravitino server URI
 *   metalake: seatunnel                 # Metalake name
 * </pre>
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
@Slf4j
@AutoService(MetadataProvider.class)
public class GravitinoMetadataProvider implements MetadataProvider {

    private String uri;
    private String metalake;
    private GravitinoClient client;
    private GravitinoTableSchemaConvertor tableSchemaConvertor;

    private static final String METALAKE_API_PATH = "api/metalakes/";
    private static final String CATALOGS_PATH = "/catalogs";
    private static final String SCHEMAS_PATH = "/schemas";
    private static final String TABLES_PATH = "/tables";

    // Gravitino JDBC property names
    private static final String GRAVITINO_JDBC_URL = "jdbc-url";
    private static final String GRAVITINO_JDBC_USER = "jdbc-user";
    private static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
    private static final String GRAVITINO_JDBC_DRIVER = "jdbc-driver";

    // SeaTunnel JDBC config names
    private static final String SEATUNNEL_URL = "url";
    private static final String SEATUNNEL_USERNAME = "username";
    private static final String SEATUNNEL_PASSWORD = "password";
    private static final String SEATUNNEL_DRIVER = "driver";

    // Supported connector identifier
    private static final String JDBC_CONNECTOR = "Jdbc";

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Gravitino server URI, e.g., http://localhost:8090");

    public static final Option<String> METALAKE =
            Options.key("metalake")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Gravitino metalake name to use for data source metadata");

    @Override
    public String kind() {
        return "gravitino";
    }

    @Override
    public void init(Config config) {
        // Extract Gravitino-specific configuration
        String uri = config.getString(URI.key());
        String metalake = config.getString(METALAKE.key());
        log.info("Gravitino server URI: {}", uri);
        log.info("Gravitino metalake name: {}", metalake);
        // Validate required parameters
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException(
                    "Gravitino URI is required. Please configure 'seatunnel.engine.datasource.uri' in seatunnel.yaml");
        }
        if (metalake == null || metalake.isEmpty()) {
            throw new IllegalArgumentException(
                    "Gravitino metalake is required. Please configure 'seatunnel.engine.datasource.metalake' in seatunnel.yaml");
        }
        this.uri = uri;
        this.metalake = metalake;
        this.client = new GravitinoClient();
        this.tableSchemaConvertor = new GravitinoTableSchemaConvertor();
    }

    @Override
    public Map<String, Object> datasourceMap(
            String connectorIdentifier, String metaDataDatasourceId) {
        if (!JDBC_CONNECTOR.equalsIgnoreCase(connectorIdentifier)) {
            log.warn(
                    "Unsupported connector '{}' for Gravitino provider, only '{}' is supported",
                    connectorIdentifier,
                    JDBC_CONNECTOR);
            return Collections.emptyMap();
        }

        try {
            String catalogBaseUrl = buildMetalakeUrl();
            JsonNode propertiesNode = client.getMetaInfo(metaDataDatasourceId, catalogBaseUrl);
            return convertToJdbcConfig(propertiesNode);
        } catch (IOException e) {
            throw new SeaTunnelException(
                    String.format(
                            "Failed to fetch metadata from Gravitino for metadata: %s",
                            metaDataDatasourceId),
                    e);
        }
    }

    @Override
    public Optional<TableSchema> tableSchema(String metaDataTableId) {
        try {
            final JsonNode tableSchema =
                    client.getTableSchema(buildMetalakeUrlTableUrl(metaDataTableId));
            return Optional.of(tableSchemaConvertor.convertor(tableSchema));
        } catch (IOException e) {
            throw new SeaTunnelException("fail get tableSchema:" + metaDataTableId, e);
        }
    }

    /**
     * Builds the metalake URL for Gravitino API calls.
     *
     * @return complete metalake URL
     */
    private String buildMetalakeUrl() {
        String baseUri = uri.endsWith("/") ? uri : uri + "/";
        return baseUri + METALAKE_API_PATH + metalake + CATALOGS_PATH;
    }

    private String buildMetalakeUrlTableUrl(String metaDataTableId) {
        final String[] split = metaDataTableId.split("\\.");
        if (split.length != 3) {
            throw new SeaTunnelException("Invalid metalake table id: " + metaDataTableId);
        }
        String catalog = split[0];
        String schema = split[1];
        String table = split[2];
        String baseUri = uri.endsWith("/") ? uri : uri + "/";
        return baseUri
                + METALAKE_API_PATH
                + metalake
                + CATALOGS_PATH
                + "/"
                + catalog
                + SCHEMAS_PATH
                + "/"
                + schema
                + TABLES_PATH
                + "/"
                + table;
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

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
