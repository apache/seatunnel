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

package org.apache.seatunnel.connectors.seatunnel.salesforce.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SalesforceSourceOptions {

    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Salesforce Connected App client ID (consumer key).");

    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Salesforce Connected App client secret (consumer secret).");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Salesforce username.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Salesforce password.");

    public static final Option<String> SECURITY_TOKEN =
            Options.key("security_token")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Salesforce security token appended to the password during "
                                    + "authentication. Leave empty if your org IP is trusted.");

    public static final Option<String> INSTANCE_URL =
            Options.key("instance_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Salesforce instance URL, e.g. https://yourorg.salesforce.com.");

    public static final Option<String> API_VERSION =
            Options.key("api_version")
                    .stringType()
                    .defaultValue("v59.0")
                    .withDescription("Salesforce REST API version, e.g. v59.0.");

    public static final Option<String> OBJECT_NAME =
            Options.key("object_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Salesforce object name for single-object mode, e.g. Account. "
                                    + "Mutually exclusive with tables_configs.");

    public static final Option<String> FILTER =
            Options.key("filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SOQL WHERE clause appended to the auto-built "
                                    + "SELECT FIELDS(ALL) FROM <object> query.");

    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("HTTP request timeout in milliseconds.");

    public static final Option<Long> POLL_INTERVAL_MS =
            Options.key("poll_interval_ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("Interval in milliseconds between Bulk API job status polls.");

    public static final Option<Long> JOB_COMPLETION_TIMEOUT_MS =
            Options.key("job_completion_timeout_ms")
                    .longType()
                    .defaultValue(3_600_000L)
                    .withDescription(
                            "Maximum time in milliseconds to wait for a Bulk API job to reach a "
                                    + "terminal state. Default 3600000 (60 minutes).");

    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Salesforce object path in 'database.ObjectName' format, "
                                    + "e.g. salesforce.Account. Used in tables_configs entries.");
}
