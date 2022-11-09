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

import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("checkstyle:MagicNumber")
public class ClickhouseConfig {

    /**
     * Bulk size of clickhouse jdbc
     */
    public static final Option<Integer> BULK_SIZE = Options.key("bulk_size").intType()
        .defaultValue(20000).withDescription("Bulk size of clickhouse jdbc");

    /**
     * Clickhouse fields
     */
    public static final Option<String> FIELDS = Options.key("fields").stringType()
        .noDefaultValue().withDescription("Clickhouse fields");

    public static final Option<String> SQL = Options.key("sql").stringType()
        .noDefaultValue().withDescription("Clickhouse sql used to query data");

    /**
     * Clickhouse server host
     */
    public static final Option<String> HOST = Options.key("host").stringType()
        .noDefaultValue().withDescription("Clickhouse server host");

    /**
     * Clickhouse table name
     */
    public static final Option<String> TABLE = Options.key("table").stringType()
        .noDefaultValue().withDescription("Clickhouse table name");

    /**
     * Clickhouse database name
     */
    public static final Option<String> DATABASE = Options.key("database").stringType()
        .noDefaultValue().withDescription("Clickhouse database name");

    /**
     * Clickhouse server username
     */
    public static final Option<String> USERNAME = Options.key("username").stringType()
        .noDefaultValue().withDescription("Clickhouse server username");

    /**
     * Clickhouse server password
     */
    public static final Option<String> PASSWORD = Options.key("password").stringType()
        .noDefaultValue().withDescription("Clickhouse server password");

    /**
     * Split mode when table is distributed engine
     */
    public static final Option<Boolean> SPLIT_MODE = Options.key("split_mode").booleanType()
        .defaultValue(false).withDescription("Split mode when table is distributed engine");

    /**
     * When split_mode is true, the sharding_key use for split
     */
    public static final Option<String> SHARDING_KEY = Options.key("sharding_key").stringType()
        .noDefaultValue().withDescription("When split_mode is true, the sharding_key use for split");

    /**
     * ClickhouseFile sink connector used clickhouse-local program's path
     */
    public static final Option<String> CLICKHOUSE_LOCAL_PATH = Options.key("clickhouse_local_path").stringType()
        .noDefaultValue().withDescription("ClickhouseFile sink connector used clickhouse-local program's path");

    /**
     * The method of copy Clickhouse file
     */
    public static final Option<ClickhouseFileCopyMethod> COPY_METHOD = Options.key("copy_method").enumType(ClickhouseFileCopyMethod.class)
        .defaultValue(ClickhouseFileCopyMethod.SCP).withDescription("The method of copy Clickhouse file");

    public static final String NODE_ADDRESS = "node_address";
    /**
     * The password of Clickhouse server node
     */
    public static final Option<List<NodePassConfig>> NODE_PASS = Options.key("node_pass").listType(NodePassConfig.class)
        .noDefaultValue().withDescription("The password of Clickhouse server node");

    public static final Option<Map<String, String>> CLICKHOUSE_PREFIX = Options.key("clickhouse").mapType()
        .defaultValue(Collections.emptyMap()).withDescription("Clickhouse custom config");

}
