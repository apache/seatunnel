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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.util.HashMap;
import java.util.Map;

public class IcebergSinkOptions extends IcebergCommonOptions {

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

    public static final Option<String> DATA_SAVE_MODE_CUSTOM_SQL =
            Options.key("custom_sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("custom delete data sql for data save mode");

    public static final Option<String> TABLES_DEFAULT_COMMIT_BRANCH =
            Options.key("iceberg.table.commit-branch")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Default branch for commits");
}
