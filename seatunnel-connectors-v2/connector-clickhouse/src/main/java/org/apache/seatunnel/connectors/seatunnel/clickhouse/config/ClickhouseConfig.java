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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClickhouseConfig {

    /** Clickhouse server host */
    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server host");

    /** Clickhouse table name */
    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse table name");

    /** Clickhouse database name */
    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse database name");

    /** Clickhouse server username */
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server username");

    /** Clickhouse server password */
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server password");

    /** Clickhouse server timezone */
    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server_time_zone")
                    .stringType()
                    .defaultValue(ZoneId.systemDefault().getId())
                    .withDescription(
                            "The session time zone in database server."
                                    + "If not set, then ZoneId.systemDefault() is used to determine the server time zone");
    /** clickhouse source sql */
    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse sql used to query data");

    /** clickhouse multi table */
    public static final Option<List<Map<String, Object>>> TABLE_LIST =
            Options.key("table_list")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("table list config");

    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table full path");

    /** Bulk size of clickhouse jdbc */
    public static final Option<Integer> BULK_SIZE =
            Options.key("bulk_size")
                    .intType()
                    .defaultValue(20000)
                    .withDescription("Bulk size of clickhouse jdbc");

    /** clickhouse conf */
    public static final Option<Map<String, String>> CLICKHOUSE_CONFIG =
            Options.key("clickhouse.config")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Clickhouse custom config");

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

    /** ClickhouseFile sink connector used clickhouse-local program's path */
    public static final Option<String> CLICKHOUSE_LOCAL_PATH =
            Options.key("clickhouse_local_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "ClickhouseFile sink connector used clickhouse-local program's path");

    /** The method of copy Clickhouse file */
    public static final Option<ClickhouseFileCopyMethod> COPY_METHOD =
            Options.key("copy_method")
                    .enumType(ClickhouseFileCopyMethod.class)
                    .defaultValue(ClickhouseFileCopyMethod.SCP)
                    .withDescription("The method of copy Clickhouse file");

    public static final Option<Boolean> COMPATIBLE_MODE =
            Options.key("compatible_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "In the lower version of Clickhouse, the ClickhouseLocal program does not support the `--path` parameter, "
                                    + "you need to use this mode to take other ways to realize the --path parameter function");

    public static final String NODE_ADDRESS = "node_address";

    public static final Option<Boolean> NODE_FREE_PASSWORD =
            Options.key("node_free_password")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Because seatunnel need to use scp or rsync for file transfer, "
                                    + "seatunnel need clickhouse server-side access. If each spark node and clickhouse server are configured with password-free login, "
                                    + "you can configure this option to true, otherwise you need to configure the corresponding node password in the node_pass configuration");
    /** The password of Clickhouse server node */
    public static final Option<List<NodePassConfig>> NODE_PASS =
            Options.key("node_pass")
                    .listType(NodePassConfig.class)
                    .noDefaultValue()
                    .withDescription("The password of Clickhouse server node");

    public static final Option<String> FILE_FIELDS_DELIMITER =
            Options.key("file_fields_delimiter")
                    .stringType()
                    .defaultValue("\t")
                    .withDescription(
                            "ClickhouseFile uses csv format to temporarily save data. If the data in the row contains the delimiter value of csv,"
                                    + " it may cause program exceptions. Avoid this with this configuration. Value string has to be an exactly one character long");

    public static final Option<String> FILE_TEMP_PATH =
            Options.key("file_temp_path")
                    .stringType()
                    .defaultValue("/tmp/seatunnel/clickhouse-local/file")
                    .withDescription(
                            "The directory where ClickhouseFile stores temporary files locally.");
}
