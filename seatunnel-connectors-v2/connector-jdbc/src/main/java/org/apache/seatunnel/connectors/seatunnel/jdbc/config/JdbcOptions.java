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

import java.math.BigDecimal;
import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public interface JdbcOptions {

    Option<String> URL = Options.key("url").stringType().noDefaultValue().withDescription("url");

    Option<String> DRIVER =
            Options.key("driver").stringType().noDefaultValue().withDescription("driver");

    Option<Integer> CONNECTION_CHECK_TIMEOUT_SEC =
            Options.key("connection_check_timeout_sec")
                    .intType()
                    .defaultValue(30)
                    .withDescription("connection check time second");
    Option<String> COMPATIBLE_MODE =
            Options.key("compatible_mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The compatible mode of database, required when the database supports multiple compatible modes. For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'.");

    Option<Integer> MAX_RETRIES =
            Options.key("max_retries").intType().defaultValue(0).withDescription("max_retired");

    Option<String> USER = Options.key("user").stringType().noDefaultValue().withDescription("user");

    Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("password");

    Option<String> QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("query");

    Option<Boolean> AUTO_COMMIT =
            Options.key("auto_commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("auto commit");

    Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(1000).withDescription("batch size");

    Option<Integer> FETCH_SIZE =
            Options.key("fetch_size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "For queries that return a large number of objects, "
                                    + "you can configure the row fetch size used in the query to improve performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value.");

    Option<Boolean> IS_EXACTLY_ONCE =
            Options.key("is_exactly_once")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("exactly once");

    Option<Boolean> GENERATE_SINK_SQL =
            Options.key("generate_sink_sql")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("generate sql using the database table");

    Option<String> XA_DATA_SOURCE_CLASS_NAME =
            Options.key("xa_data_source_class_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("data source class name");

    Option<Integer> MAX_COMMIT_ATTEMPTS =
            Options.key("max_commit_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("max commit attempts");

    Option<Integer> TRANSACTION_TIMEOUT_SEC =
            Options.key("transaction_timeout_sec")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("transaction timeout (second)");

    Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");

    Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table");

    Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys").listType().noDefaultValue().withDescription("primary keys");

    Option<Boolean> SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST =
            Options.key("support_upsert_by_query_primary_key_exist")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("support upsert by query primary_key exist");

    Option<Boolean> ENABLE_UPSERT =
            Options.key("enable_upsert")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable upsert by primary_keys exist");
    Option<Boolean> IS_PRIMARY_KEY_UPDATED =
            Options.key("is_primary_key_updated")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "is the primary key updated when performing an update operation");
    Option<Boolean> SUPPORT_UPSERT_BY_INSERT_ONLY =
            Options.key("support_upsert_by_insert_only")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("support upsert by insert only");

    /** source config */
    Option<String> PARTITION_COLUMN =
            Options.key("partition_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("partition column");

    Option<BigDecimal> PARTITION_UPPER_BOUND =
            Options.key("partition_upper_bound")
                    .bigDecimalType()
                    .noDefaultValue()
                    .withDescription("partition upper bound");
    Option<BigDecimal> PARTITION_LOWER_BOUND =
            Options.key("partition_lower_bound")
                    .bigDecimalType()
                    .noDefaultValue()
                    .withDescription("partition lower bound");
    Option<Integer> PARTITION_NUM =
            Options.key("partition_num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("partition num");
}
