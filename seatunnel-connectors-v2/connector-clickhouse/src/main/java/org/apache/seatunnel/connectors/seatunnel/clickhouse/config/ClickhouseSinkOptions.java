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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

public class ClickhouseSinkOptions {

    /** Bulk size of clickhouse jdbc */
    public static final Option<Integer> BULK_SIZE =
            Options.key("bulk_size")
                    .intType()
                    .defaultValue(20000)
                    .withDescription("Bulk size of clickhouse jdbc");

    /** Clickhouse table name */
    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse table name");

    /** Split mode when table is distributed engine */
    public static final Option<Boolean> SPLIT_MODE =
            Options.key("split_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Split mode when table is distributed engine");

    /** When split_mode is true, the sharding_key use for split */
    public static final Option<String> SHARDING_KEY =
            Options.key("sharding_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("When split_mode is true, the sharding_key use for split");

    public static final Option<String> PRIMARY_KEY =
            Options.key("primary_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Mark the primary key column from clickhouse table, and based on primary key execute INSERT/UPDATE/DELETE to clickhouse table");

    public static final Option<Boolean> SUPPORT_UPSERT =
            Options.key("support_upsert")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Support upsert row by query primary key");

    public static final Option<Boolean> ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE =
            Options.key("allow_experimental_lightweight_delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow experimental lightweight delete based on `*MergeTree` table engine");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription(
                            "different treatment schemes are selected for the existing surface structure of the target side");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription(
                            "different processing schemes are selected for data existing data on the target side");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("when data_save_mode selects CUSTOM_PROCESSING custom SQL");

    public static final Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "CREATE TABLE IF NOT EXISTS `"
                                    + SaveModePlaceHolder.DATABASE.getPlaceHolder()
                                    + "`.`"
                                    + SaveModePlaceHolder.TABLE.getPlaceHolder()
                                    + "` (\n"
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ",\n"
                                    + SaveModePlaceHolder.ROWTYPE_FIELDS.getPlaceHolder()
                                    + "\n"
                                    + ") ENGINE = MergeTree()\n"
                                    + "ORDER BY ("
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ")\n"
                                    + "PRIMARY KEY ("
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ")\n"
                                    + "SETTINGS\n"
                                    + "    index_granularity = 8192"
                                    + "\n"
                                    + "COMMENT '"
                                    + SaveModePlaceHolder.COMMENT.getPlaceHolder()
                                    + "';")
                    .withDescription(
                            "Create table statement template, used to create Clickhouse table");
}
