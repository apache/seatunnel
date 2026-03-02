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

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.TableIdentifierOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.redis.source.RedisSourceTable;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
@JsonIgnoreProperties(ignoreUnknown = true)
public class RedisTableConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("table_path")
    private String tablePath;

    @JsonProperty("keys")
    private String keys;

    @JsonProperty("data_type")
    private RedisDataType dataType;

    @JsonProperty("batch_size")
    private int batchSize;

    @JsonProperty("format")
    private RedisBaseOptions.Format format;

    @JsonProperty("schema")
    private Map<String, Object> schema;

    @JsonProperty("hash_key_parse_mode")
    private RedisSourceOptions.HashKeyParseMode hashKeyParseMode;

    @JsonProperty("read_key_enabled")
    private Boolean readKeyEnabled;

    @JsonProperty("key_field_name")
    private String keyFieldName;

    @JsonProperty("single_field_name")
    private String singleFieldName;

    @JsonProperty("field_delimiter")
    private String fieldDelimiter;

    @Tolerate
    public RedisTableConfig() {}

    /** Convert this configuration to a RedisSourceTable with runtime metadata. */
    public RedisSourceTable toSourceTable(
            TablePath tablePath,
            CatalogTable catalogTable,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        return RedisSourceTable.builder()
                .tablePath(tablePath)
                .keyPattern(this.keys)
                .dataType(this.dataType)
                .batchSize(this.batchSize)
                .catalogTable(catalogTable)
                .deserializationSchema(deserializationSchema)
                .hashKeyParseMode(this.hashKeyParseMode)
                .readKeyEnabled(this.readKeyEnabled)
                .keyFieldName(this.keyFieldName)
                .singleFieldName(this.singleFieldName)
                .build();
    }

    /** Get TablePath from configuration. */
    public TablePath getTablePath(ReadonlyConfig readonlyConfig, String keys) {
        ReadonlyConfig schemaConfig =
                readonlyConfig
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
            List<RedisTableConfig> tableList = config.get(TABLE_LIST);
            validateAndInitializeTables(tableList);
            return tableList;
        } else {
            // Single table mode (backward compatibility)
            return Collections.singletonList(buildSingleTableConfig(config));
        }
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

        RedisTableConfig tableConfig =
                RedisTableConfig.builder()
                        .keys(config.get(KEY_PATTERN))
                        .dataType(config.get(DATA_TYPE))
                        .batchSize(config.get(BATCH_SIZE))
                        .format(config.get(FORMAT))
                        .schema(
                                config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()
                                        ? config.get(ConnectorCommonOptions.SCHEMA)
                                        : null)
                        .hashKeyParseMode(config.get(HASH_KEY_PARSE_MODE))
                        .readKeyEnabled(config.get(READ_KEY_ENABLED))
                        .keyFieldName(
                                config.getOptional(KEY_FIELD_NAME).isPresent()
                                        ? config.get(KEY_FIELD_NAME)
                                        : null)
                        .singleFieldName(
                                config.getOptional(SINGLE_FIELD_NAME).isPresent()
                                        ? config.get(SINGLE_FIELD_NAME)
                                        : null)
                        .fieldDelimiter(config.get(FIELD_DELIMITER))
                        .build();

        validateTableConfig(tableConfig);
        return tableConfig;
    }

    /**
     * Validate and initialize table configurations.
     *
     * @param tableList List of RedisTableConfig
     */
    private static void validateAndInitializeTables(List<RedisTableConfig> tableList) {
        if (tableList == null || tableList.isEmpty()) {
            throw new IllegalArgumentException("table_list cannot be empty.");
        }

        for (RedisTableConfig tableConfig : tableList) {
            validateTableConfig(tableConfig);

            // Set default values
            if (tableConfig.getBatchSize() <= 0) {
                tableConfig.setBatchSize(BATCH_SIZE.defaultValue());
            }
            if (tableConfig.getFormat() == null) {
                tableConfig.setFormat(FORMAT.defaultValue());
            }
            if (tableConfig.getHashKeyParseMode() == null) {
                tableConfig.setHashKeyParseMode(HASH_KEY_PARSE_MODE.defaultValue());
            }
            if (tableConfig.getReadKeyEnabled() == null) {
                tableConfig.setReadKeyEnabled(READ_KEY_ENABLED.defaultValue());
            }
            if (tableConfig.getFieldDelimiter() == null) {
                tableConfig.setFieldDelimiter(FIELD_DELIMITER.defaultValue());
            }
        }
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
