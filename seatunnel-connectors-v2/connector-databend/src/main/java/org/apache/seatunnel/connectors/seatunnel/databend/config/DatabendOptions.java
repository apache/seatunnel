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

package org.apache.seatunnel.connectors.seatunnel.databend.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class DatabendOptions {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The URL of the Databend database in standard JDBC format");

    public static final Option<Boolean> SSL =
            Options.key("ssl")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use SSL for the Databend connection");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for Databend database authentication");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for Databend database authentication");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the Databend database to connect to");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the Databend table to read or write data");

    public static final Option<Map<String, String>> JDBC_CONFIG =
            Options.key("jdbc_config")
                    .mapType()
                    .defaultValue(null)
                    .withDescription("The additional JDBC connection configuration");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The batch size for writing to Databend");

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch_size")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "For queries that return a large number of objects, "
                                    + "you can configure the row fetch size used in the query to improve performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value.");

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The SQL query used to read data from Databend");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retries for Databend client");

    public static final Option<Boolean> AUTO_COMMIT =
            Options.key("auto_commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to auto commit for sink");
}
