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

package org.apache.seatunnel.connectors.seatunnel.dsql.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

public class DSQLSinkOptions {

    public static final Option<String> CLUSTER_ENDPOINT =
            Options.key("cluster_endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("DSQL cluster endpoint ARN or URL");

    public static final Option<String> DATABASE_NAME =
            Options.key("database_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("DSQL database name");

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target table name in DSQL");

    public static final Option<String> AWS_REGION =
            Options.key("aws_region")
                    .stringType()
                    .defaultValue("us-east-1")
                    .withDescription("AWS region for DSQL cluster");

    public static final Option<String> ACCESS_KEY_ID =
            Options.key("access_key_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS access key ID");

    public static final Option<String> SECRET_ACCESS_KEY =
            Options.key("secret_access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS secret access key");

    //    public static final Option<String> SESSION_TOKEN =
    //            Options.key("session_token")
    //                    .stringType()
    //                    .noDefaultValue()
    //                    .withDescription("AWS session token (optional for temporary
    // credentials)");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Batch size for bulk insert operations");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries for failed operations");

    public static final Option<Long> RETRY_DELAY_MS =
            Options.key("retry_delay_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Initial delay between retries in milliseconds (will use exponential backoff)");

    public static final Option<Boolean> CREATE_TABLE_IF_NOT_EXISTS =
            Options.key("create_table_if_not_exists")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Create table if it doesn't exist");

    public static final Option<Integer> CONNECTION_TIMEOUT_MS =
            Options.key("connection_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Connection timeout in milliseconds");

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Socket timeout in milliseconds");

    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "List of column names to use as primary keys when creating a table");

    public static final Option<String> PROFILE_NAME =
            Options.key("profile_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "AWS profile name to use for credentials (alternative to access_key_id/secret_access_key)");

    public static final Option<String> USER_NAME =
            Options.key("db_user_name")
                    .stringType()
                    .defaultValue("admin")
                    .withDescription("The database user name");
    public static final Option<Boolean> ENABLE_MULTI_TABLE =
            Options.key("enable_multi_table")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable multi-table synchronization mode");

    // Multi-table support options
    public static final Option<Map<String, String>> TABLE_MAPPING =
            Options.key("table_mapping")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Mapping of source table names to target table names. Format: {\"source_table1\": \"target_table1\", \"source_table2\": \"target_table2\"}");
}
