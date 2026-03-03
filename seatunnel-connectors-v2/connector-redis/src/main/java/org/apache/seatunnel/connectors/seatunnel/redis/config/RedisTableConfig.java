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

package org.apache.seatunnel.connectors.seatunnel.redis.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.TableIdentifierOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions.DATA_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions.KEY_PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions.HASH_KEY_PARSE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions.KEY_FIELD_NAME;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions.READ_KEY_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions.SINGLE_FIELD_NAME;
import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions.TABLE_LIST;

@Data
@Builder
public class RedisTableConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String keys;
    private RedisDataType dataType;
    private int batchSize;
    private RedisBaseOptions.Format format;
    private Map<String, Object> schema;
    private RedisSourceOptions.HashKeyParseMode hashKeyParseMode;
    private Boolean readKeyEnabled;
    private String keyFieldName;
    private String singleFieldName;
    private String fieldDelimiter;

    private TablePath tablePath;
    private CatalogTable catalogTable;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Tolerate
    public RedisTableConfig() {}

    /**
     * Get TablePath from table-level configuration.
     *
     * @param tableConfig ReadonlyConfig for a single table
     * @param keys Redis key pattern
     * @return TablePath
     */
    private static TablePath getTablePath(ReadonlyConfig tableConfig, String keys) {
        ReadonlyConfig schemaConfig =
                tableConfig
                        .getOptional(TableSchemaOptions.SCHEMA)
                        .map(ReadonlyConfig::fromMap)
                        .orElse(ReadonlyConfig.fromMap(Collections.emptyMap()));

        return schemaConfig
                .getOptional(TableIdentifierOptions.TABLE)
                .map(TablePath::of)
                .orElseGet(() -> TablePath.of(null, keys));
    }

    /**
     * Parse table configurations from ReadonlyConfig, supporting both single table and multi-table
     * modes.
     *
     * @param config ReadonlyConfig instance
     * @return List of RedisTableConfig
     */
    public static List<RedisTableConfig> of(ReadonlyConfig config) {
        // Check if using multi-table mode
        if (config.getOptional(TABLE_LIST).isPresent()) {
            List<Map<String, Object>> tableListMaps = config.get(TABLE_LIST);
            return tableListMaps.stream()
                    .map(ReadonlyConfig::fromMap) // Map → ReadonlyConfig
                    .map(RedisTableConfig::buildFromConfig) // Manual construction
                    .collect(Collectors.toList());
        } else {
            // Single table mode (backward compatibility)
            return Collections.singletonList(buildSingleTableConfig(config));
        }
    }

    /**
     * Build RedisTableConfig from ReadonlyConfig (for multi-table mode).
     *
     * @param tableConfig ReadonlyConfig for a single table (table-level config)
     * @return Fully initialized RedisTableConfig with runtime objects
     */
    private static RedisTableConfig buildFromConfig(ReadonlyConfig tableConfig) {
        // Validate required fields
        if (!tableConfig.getOptional(KEY_PATTERN).isPresent()) {
            throw new IllegalArgumentException(
                    "Table configuration must specify 'keys' parameter.");
        }
        if (!tableConfig.getOptional(DATA_TYPE).isPresent()) {
            throw new IllegalArgumentException(
                    "Table configuration must specify 'data_type' parameter.");
        }

        // Initialize CatalogTable and DeserializationSchema
        CatalogTable catalogTable;
        DeserializationSchema<SeaTunnelRow> deserializationSchema;

        if (tableConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(tableConfig);
            SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();

            // Create deserialization schema based on format
            RedisBaseOptions.Format format = tableConfig.get(FORMAT);
            switch (format) {
                case JSON:
                    deserializationSchema =
                            new JsonDeserializationSchema(catalogTable, false, false);
                    break;
                case TEXT:
                    deserializationSchema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(seaTunnelRowType)
                                    .delimiter(tableConfig.get(FIELD_DELIMITER))
                                    .build();
                    break;
                default:
                    throw new RedisConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT, "Unsupported format: " + format);
            }
        } else {
            // No schema specified, use simple text table
            catalogTable = CatalogTableUtil.buildSimpleTextTable();
            deserializationSchema = null;
        }

        // Initialize TablePath
        String keys = tableConfig.get(KEY_PATTERN);
        TablePath tablePath = getTablePath(tableConfig, keys);

        return RedisTableConfig.builder()
                .keys(keys)
                .dataType(tableConfig.get(DATA_TYPE)) // ← Goes through convertToEnum()
                .batchSize(tableConfig.get(BATCH_SIZE))
                .format(tableConfig.get(FORMAT)) // ← Goes through convertToEnum()
                .schema(tableConfig.getOptional(ConnectorCommonOptions.SCHEMA).orElse(null))
                .hashKeyParseMode(
                        tableConfig.get(HASH_KEY_PARSE_MODE)) // ← Goes through convertToEnum()
                .readKeyEnabled(tableConfig.get(READ_KEY_ENABLED))
                .keyFieldName(tableConfig.getOptional(KEY_FIELD_NAME).orElse(null))
                .singleFieldName(tableConfig.getOptional(SINGLE_FIELD_NAME).orElse(null))
                .fieldDelimiter(tableConfig.get(FIELD_DELIMITER))
                .tablePath(tablePath)
                .catalogTable(catalogTable)
                .deserializationSchema(deserializationSchema)
                .build();
    }

    /**
     * Build a single table configuration from global config (backward compatibility).
     *
     * @param config ReadonlyConfig instance
     * @return RedisTableConfig
     */
    private static RedisTableConfig buildSingleTableConfig(ReadonlyConfig config) {
        // Validate that required fields for single table mode are present
        if (!config.getOptional(KEY_PATTERN).isPresent()) {
            throw new IllegalArgumentException(
                    "Single table mode requires 'keys' parameter. "
                            + "Use 'table_list' for multi-table mode.");
        }
        if (!config.getOptional(DATA_TYPE).isPresent()) {
            throw new IllegalArgumentException(
                    "Single table mode requires 'data_type' parameter. "
                            + "Use 'table_list' for multi-table mode.");
        }

        // Initialize CatalogTable and DeserializationSchema
        CatalogTable catalogTable;
        DeserializationSchema<SeaTunnelRow> deserializationSchema;

        if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            // Build catalog table from config
            catalogTable = CatalogTableUtil.buildWithConfig(config);
            SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();

            // Create deserialization schema based on format
            RedisBaseOptions.Format format = config.get(FORMAT);
            switch (format) {
                case JSON:
                    deserializationSchema =
                            new JsonDeserializationSchema(catalogTable, false, false);
                    break;
                case TEXT:
                    deserializationSchema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(seaTunnelRowType)
                                    .delimiter(config.get(FIELD_DELIMITER))
                                    .build();
                    break;
                default:
                    throw new RedisConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT, "Unsupported format: " + format);
            }
        } else {
            // No schema specified, use simple text table
            catalogTable = CatalogTableUtil.buildSimpleTextTable();
            deserializationSchema = null;
        }

        // Initialize TablePath
        String keys = config.get(KEY_PATTERN);
        TablePath tablePath = getTablePath(config, keys);

        RedisTableConfig tableConfig =
                RedisTableConfig.builder()
                        .keys(keys)
                        .dataType(config.get(DATA_TYPE))
                        .batchSize(config.get(BATCH_SIZE))
                        .format(config.get(FORMAT))
                        .schema(config.getOptional(ConnectorCommonOptions.SCHEMA).orElse(null))
                        .hashKeyParseMode(config.get(HASH_KEY_PARSE_MODE))
                        .readKeyEnabled(config.get(READ_KEY_ENABLED))
                        .keyFieldName(config.getOptional(KEY_FIELD_NAME).orElse(null))
                        .singleFieldName(config.getOptional(SINGLE_FIELD_NAME).orElse(null))
                        .fieldDelimiter(config.get(FIELD_DELIMITER))
                        .tablePath(tablePath)
                        .catalogTable(catalogTable)
                        .deserializationSchema(deserializationSchema)
                        .build();

        validateTableConfig(tableConfig);
        return tableConfig;
    }

    /**
     * Validate a single table configuration.
     *
     * @param tableConfig RedisTableConfig to validate
     */
    private static void validateTableConfig(RedisTableConfig tableConfig) {
        if (tableConfig.getKeys() == null || tableConfig.getKeys().trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Table configuration must specify 'keys' parameter.");
        }
        if (tableConfig.getDataType() == null) {
            throw new IllegalArgumentException(
                    "Table configuration must specify 'data_type' parameter.");
        }
    }
}
