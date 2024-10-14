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

package org.apache.seatunnel.connectors.seatunnel.timeplus.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** The Seatunnel connector configuration. A set of {@link Option}. */
public class TimeplusConfig {

    /** Bulk size of timeplus jdbc */
    public static final Option<Integer> BULK_SIZE =
            Options.key("bulk_size")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Bulk size of timeplus jdbc");

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Timeplus sql used to query data");

    /** Timeplus server host */
    // TODO: rename to nodeUrls in listType
    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .defaultValue("localhost:8123")
                    .withDescription("Timeplus server host and port, default localhost:8123");

    /** Timeplus table name */
    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .defaultValue("${table_name}")
                    .withDescription("Timeplus table name");

    /** Timeplus database name */
    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Timeplus database name");

    /** Timeplus server username */
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Timeplus server username");

    /** Timeplus server password */
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Timeplus server password");

    /** Timeplus server timezone */
    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server_time_zone")
                    .stringType()
                    .defaultValue(ZoneId.systemDefault().getId())
                    .withDescription(
                            "The session time zone in database server."
                                    + "If not set, then ZoneId.systemDefault() is used to determine the server time zone");

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
                            "Mark the primary key column from timeplus table, and based on primary key execute INSERT/UPDATE/DELETE to timeplus table");

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

    /** TimeplusFile sink connector used timeplus-local program's path */
    public static final Option<String> TIMEPLUS_LOCAL_PATH =
            Options.key("timeplus_local_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "TimeplusFile sink connector used timeplus-local program's path");

    /** The method of copy Timeplus file */
    public static final Option<TimeplusFileCopyMethod> COPY_METHOD =
            Options.key("copy_method")
                    .enumType(TimeplusFileCopyMethod.class)
                    .defaultValue(TimeplusFileCopyMethod.SCP)
                    .withDescription("The method of copy Timeplus file");

    public static final Option<Boolean> COMPATIBLE_MODE =
            Options.key("compatible_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "In the lower version of Timeplus, the TimeplusLocal program does not support the `--path` parameter, "
                                    + "you need to use this mode to take other ways to realize the --path parameter function");

    public static final String NODE_ADDRESS = "node_address";

    public static final Option<Boolean> NODE_FREE_PASSWORD =
            Options.key("node_free_password")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Because seatunnel need to use scp or rsync for file transfer, "
                                    + "seatunnel need timeplus server-side access. If each spark node and timeplus server are configured with password-free login, "
                                    + "you can configure this option to true, otherwise you need to configure the corresponding node password in the node_pass configuration");

    /** The password of Timeplus server node */
    public static final Option<List<NodePassConfig>> NODE_PASS =
            Options.key("node_pass")
                    .listType(NodePassConfig.class)
                    .noDefaultValue()
                    .withDescription("The password of Timeplus server node");

    public static final Option<Map<String, String>> TIMEPLUS_CONFIG =
            Options.key("timeplus.config")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Timeplus custom config");

    public static final Option<String> FILE_FIELDS_DELIMITER =
            Options.key("file_fields_delimiter")
                    .stringType()
                    .defaultValue("\t")
                    .withDescription(
                            "TimeplusFile uses csv format to temporarily save data. If the data in the row contains the delimiter value of csv,"
                                    + " it may cause program exceptions. Avoid this with this configuration. Value string has to be an exactly one character long");

    public static final Option<String> FILE_TEMP_PATH =
            Options.key("file_temp_path")
                    .stringType()
                    .defaultValue("/tmp/seatunnel/timeplus-local/file")
                    .withDescription(
                            "The directory where TimeplusFile stores temporary files locally.");

    public static final Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "CREATE STREAM IF NOT EXISTS `"
                                    + SaveModePlaceHolder.DATABASE.getPlaceHolder()
                                    + "`.`"
                                    + SaveModePlaceHolder.TABLE_NAME.getPlaceHolder()
                                    + "` (\n"
                                    + SaveModePlaceHolder.ROWTYPE_FIELDS.getPlaceHolder()
                                    + "\n)\n"
                                    + "SETTINGS logstore_retention_bytes=1073741824, logstore_retention_ms=3600000") // default 1GB or 1hour
                    .withDescription(
                            "Create table statement template, used to create Timeplus stream");

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
}
