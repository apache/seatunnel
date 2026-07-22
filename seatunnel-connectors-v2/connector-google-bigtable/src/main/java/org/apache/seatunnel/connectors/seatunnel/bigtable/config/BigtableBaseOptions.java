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

package org.apache.seatunnel.connectors.seatunnel.bigtable.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.util.List;

public class BigtableBaseOptions extends ConnectorCommonOptions {

    public static final Option<String> PROJECT_ID =
            Options.key("project_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google Cloud project ID");

    public static final Option<String> INSTANCE_ID =
            Options.key("instance_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bigtable instance ID");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bigtable table name");

    public static final Option<List<String>> ROWKEY_COLUMNS =
            Options.key("rowkey_column")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Column names used to compose the Bigtable row key. "
                                    + "If multiple columns are specified they are joined with rowkey_delimiter.");

    public static final Option<String> CREDENTIALS_PATH =
            Options.key("credentials_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to the Google Cloud service account JSON key file. "
                                    + "If not set, Application Default Credentials (ADC) will be used.");
}
