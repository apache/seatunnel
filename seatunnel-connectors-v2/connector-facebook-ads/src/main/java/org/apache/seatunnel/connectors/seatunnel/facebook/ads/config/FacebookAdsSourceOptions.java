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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

public class FacebookAdsSourceOptions {

    public static final Option<String> ACCESS_TOKEN =
            Options.key("access_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Meta Marketing API access token with the ads_read permission, "
                                    + "e.g. a long-lived user token or a system user token.");

    public static final Option<String> AD_ACCOUNT_ID =
            Options.key("ad_account_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Facebook ad account ID to query, digits only, e.g. 1234567890. "
                                    + "A leading act_ prefix is accepted and stripped.");

    public static final Option<String> API_VERSION =
            Options.key("api_version")
                    .stringType()
                    .defaultValue("v23.0")
                    .withDescription("Facebook Graph API version, e.g. v23.0.");

    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Ad account edge to read in single-table mode, e.g. campaigns, "
                                    + "adsets, ads, insights. Mutually exclusive with "
                                    + "tables_configs.");

    public static final Option<List<String>> FIELDS =
            Options.key("fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Ordered list of field names to select, e.g. [id, name, status]. "
                                    + "The output schema follows this order.");

    public static final Option<String> FILTERING =
            Options.key("filtering")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JSON array passed as the Graph API filtering parameter, e.g. "
                                    + "[{\"field\":\"effective_status\",\"operator\":\"IN\","
                                    + "\"value\":[\"ACTIVE\"]}].");

    public static final Option<Map<String, String>> PARAMS =
            Options.key("params")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Extra query parameters appended to the request, e.g. "
                                    + "{date_preset = last_30d, level = campaign} for the "
                                    + "insights edge.");

    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("HTTP request timeout in milliseconds for a single call.");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum retries for transient HTTP failures (429/5xx/rate "
                                    + "limits/network errors) of a single request.");

    public static final Option<Long> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Base backoff in milliseconds between retries; doubled per attempt.");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Page size for each request (the Graph API limit parameter). "
                                    + "When unset the server default is used.");

    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table path in 'database.resource' format, e.g. "
                                    + "facebook_ads.campaigns. Used in tables_configs entries.");

    public static final Option<String> API_ENDPOINT =
            Options.key("api_endpoint")
                    .stringType()
                    .defaultValue("https://graph.facebook.com")
                    .withDescription("Facebook Graph API base endpoint. Intended for testing.");
}
