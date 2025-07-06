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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import java.util.List;

public class JdbcSinkOptions extends JdbcCommonOptions {

    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("data_save_mode");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql").stringType().noDefaultValue().withDescription("custom_sql");

    public static final Option<Boolean> GENERATE_SINK_SQL =
            Options.key("generate_sink_sql")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("generate sql using the database table");

    public static final Option<Boolean> IS_EXACTLY_ONCE =
            Options.key("is_exactly_once")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("exactly once");

    public static final Option<Boolean> AUTO_COMMIT =
            Options.key("auto_commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("auto commit");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries").intType().defaultValue(0).withDescription("max_retired");

    public static final Option<String> XA_DATA_SOURCE_CLASS_NAME =
            Options.key("xa_data_source_class_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("data source class name");

    public static final Option<Integer> MAX_COMMIT_ATTEMPTS =
            Options.key("max_commit_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("max commit attempts");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(1000).withDescription("batch size");

    public static final Option<Integer> TRANSACTION_TIMEOUT_SEC =
            Options.key("transaction_timeout_sec")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("transaction timeout (second)");

    public static final Option<Boolean> ENABLE_UPSERT =
            Options.key("enable_upsert")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable upsert by primary_keys exist");

    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys").listType().noDefaultValue().withDescription("primary keys");

    public static final Option<Boolean> IS_PRIMARY_KEY_UPDATED =
            Options.key("is_primary_key_updated")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "is the primary key updated when performing an update operation");

    public static final Option<Boolean> SUPPORT_UPSERT_BY_INSERT_ONLY =
            Options.key("support_upsert_by_insert_only")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("support upsert by insert only");

    public static final Option<Boolean> USE_COPY_STATEMENT =
            Options.key("use_copy_statement")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("support copy in statement (postgresql)");

    public static final Option<FieldIdeEnum> FIELD_IDE =
            Options.key("field_ide")
                    .enumType(FieldIdeEnum.class)
                    .noDefaultValue()
                    .withDescription("Whether case conversion is required");

    public static final Option<String> TABLE_PREFIX =
            Options.key("tablePrefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table prefix name added when the table is automatically created");

    public static final Option<String> TABLE_SUFFIX =
            Options.key("tableSuffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table suffix name added when the table is automatically created");

    public static final Option<Boolean> CREATE_INDEX =
            Options.key("create_index")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Create index or not when auto create table");
}
