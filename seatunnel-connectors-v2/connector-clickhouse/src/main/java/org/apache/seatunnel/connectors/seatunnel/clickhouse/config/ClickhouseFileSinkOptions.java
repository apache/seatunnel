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

import java.util.List;

public class ClickhouseFileSinkOptions {
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

    public static final Option<String> KEY_PATH =
            Options.key("key_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of rsync/ssh key file");

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
