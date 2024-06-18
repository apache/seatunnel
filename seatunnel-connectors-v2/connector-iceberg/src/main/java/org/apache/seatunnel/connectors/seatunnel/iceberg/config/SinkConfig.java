/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@Getter
@Setter
@ToString
public class SinkConfig extends CommonConfig {

    public static final int SCHEMA_UPDATE_RETRIES = 2; // 3 total attempts
    public static final int CREATE_TABLE_RETRIES = 2; // 3 total attempts

    private static final String ID_COLUMNS = "id-columns";
    private static final String PARTITION_BY = "partition-by";

    public static final Option<Map<String, String>> TABLE_PROPS =
            Options.key("iceberg.table.config")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("Iceberg table configs");

    public static final Option<Map<String, String>> WRITE_PROPS =
            Options.key("iceberg.table.write-props")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Properties passed through to Iceberg writer initialization, these take precedence, such as 'write.format.default', 'write.target-file-size-bytes', and other settings, can be found with specific parameters at 'https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/TableProperties.java'.");

    public static final Option<Map<String, String>> AUTO_CREATE_PROPS =
            Options.key("iceberg.table.auto-create-props")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Configuration specified by Iceberg during automatic table creation.");

    public static final Option<Boolean> TABLE_SCHEMA_EVOLUTION_ENABLED_PROP =
            Options.key("iceberg.table.schema-evolution-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Setting to true enables Iceberg tables to support schema evolution during the synchronization process");

    public static final Option<String> TABLE_PRIMARY_KEYS =
            Options.key("iceberg.table.primary-keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Default comma-separated list of columns that identify a row in tables (primary key)");

    public static final Option<String> TABLE_DEFAULT_PARTITION_KEYS =
            Options.key("iceberg.table.partition-keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Default comma-separated list of partition fields to use when creating tables.");

    public static final Option<Boolean> TABLE_UPSERT_MODE_ENABLED_PROP =
            Options.key("iceberg.table.upsert-mode-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Set to `true` to enable upsert mode, default is `false`");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema save mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("data save mode");

    public static final Option<String> TABLES_DEFAULT_COMMIT_BRANCH =
            Options.key("iceberg.table.commit-branch")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Default branch for commits");

    @VisibleForTesting private static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

    private final ReadonlyConfig readonlyConfig;
    private Map<String, String> autoCreateProps;
    private Map<String, String> writeProps;
    private List<String> primaryKeys;
    private List<String> partitionKeys;
    private String commitBranch;

    private boolean upsertModeEnabled;
    private boolean tableSchemaEvolutionEnabled;
    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;

    public SinkConfig(ReadonlyConfig readonlyConfig) {
        super(readonlyConfig);
        this.readonlyConfig = readonlyConfig;
        this.autoCreateProps = readonlyConfig.get(AUTO_CREATE_PROPS);
        this.writeProps = readonlyConfig.get(WRITE_PROPS);
        this.primaryKeys = stringToList(readonlyConfig.get(TABLE_PRIMARY_KEYS), ",");
        this.partitionKeys = stringToList(readonlyConfig.get(TABLE_DEFAULT_PARTITION_KEYS), ",");
        this.upsertModeEnabled = readonlyConfig.get(TABLE_UPSERT_MODE_ENABLED_PROP);
        this.tableSchemaEvolutionEnabled = readonlyConfig.get(TABLE_SCHEMA_EVOLUTION_ENABLED_PROP);
        this.schemaSaveMode = readonlyConfig.get(SCHEMA_SAVE_MODE);
        this.dataSaveMode = readonlyConfig.get(DATA_SAVE_MODE);
        this.commitBranch = readonlyConfig.get(TABLES_DEFAULT_COMMIT_BRANCH);
    }

    @VisibleForTesting
    public static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return ImmutableList.of();
        }
        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }
}
