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

package org.apache.seatunnel.connectors.bigquery.option;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

public class BigQuerySinkOptions {

    public static final String IDENTIFIER = "BigQuery";

    public static final Option<String> PROJECT_ID =
            Options.key("project_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("GCP project ID");

    public static final Option<String> DATASET_ID =
            Options.key("dataset_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BigQuery dataset ID");

    public static final Option<String> TABLE_ID =
            Options.key("table_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BigQuery table ID");

    public static final Option<String> SERVICE_ACCOUNT_KEY_PATH =
            Options.key("service_account_key_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to GCP service account JSON key file");

    public static final Option<String> SERVICE_ACCOUNT_KEY_JSON =
            Options.key("service_account_key_json")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inline GCP service account JSON key content");

    public static final Option<String> WRITE_MODE =
            Options.key("write_mode")
                    .stringType()
                    .defaultValue("batch")
                    .withDescription(
                            "The write mode to use when writing to BigQuery. Supported values are 'batch' and 'streaming'.");

    public static final Option<String> SEQUENCE_NUMBER_COLUMN =
            Options.key("sequence_number_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the column used to store the sequence number for change data capture. Only applicable when write_mode is 'streaming'.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The number of rows sent in a single batch");

    public static final Option<String> EMULATOR_HOST =
            Options.key("emulator_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The host of the BigQuery emulator (e.g. localhost:9050). Only for testing purposes.");

    public static final Option<String> UNIVERSE_DOMAIN =
            Options.key("universe_domain")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud Universe Domain (e.g. s3nsapis.fr for S3NS sovereign cloud)");

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

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("custom SQL to execute for CUSTOM_PROCESSING data save mode");
}
