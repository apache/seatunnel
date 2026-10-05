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

package org.apache.seatunnel.connectors.seatunnel.google.ads.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class GoogleAdsSourceOptions {

    public static final Option<String> DEVELOPER_TOKEN =
            Options.key("developer_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Google Ads API developer token, obtained from a Google Ads "
                                    + "manager (MCC) account.");

    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OAuth2 client ID of the Google Cloud application.");

    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OAuth2 client secret of the Google Cloud application.");

    public static final Option<String> REFRESH_TOKEN =
            Options.key("refresh_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "OAuth2 refresh token authorized for the Google Ads API scope "
                                    + "(https://www.googleapis.com/auth/adwords).");

    public static final Option<String> CUSTOMER_ID =
            Options.key("customer_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Google Ads customer ID to query, digits only without dashes, "
                                    + "e.g. 1234567890.");

    public static final Option<String> LOGIN_CUSTOMER_ID =
            Options.key("login_customer_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Manager (MCC) account customer ID, digits only. Required when "
                                    + "customer_id is a client account managed by an MCC.");

    public static final Option<String> API_VERSION =
            Options.key("api_version")
                    .stringType()
                    .defaultValue("v21")
                    .withDescription("Google Ads REST API version, e.g. v21.");

    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Google Ads resource to query in single-table mode, e.g. campaign, "
                                    + "ad_group, keyword_view. Mutually exclusive with query and "
                                    + "tables_configs.");

    public static final Option<List<String>> FIELDS =
            Options.key("fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Ordered list of GAQL field paths to select, e.g. "
                                    + "[campaign.id, campaign.name, metrics.clicks]. The output "
                                    + "schema follows this order.");

    public static final Option<String> FILTER =
            Options.key("filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "GAQL WHERE clause appended to the auto-built "
                                    + "SELECT <fields> FROM <resource> query, e.g. "
                                    + "segments.date DURING LAST_30_DAYS.");

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Full GAQL query. Mutually exclusive with resource/fields/filter. "
                                    + "The output schema follows the SELECT field order.");

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
                            "Maximum retries for transient HTTP failures "
                                    + "(429/5xx/network errors) of a single request.");

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
                            "Page size for search requests. When unset the server default "
                                    + "is used.");

    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table path in 'database.resource' format, e.g. google_ads.campaign. "
                                    + "Used in tables_configs entries.");

    public static final Option<String> API_ENDPOINT =
            Options.key("api_endpoint")
                    .stringType()
                    .defaultValue("https://googleads.googleapis.com")
                    .withDescription("Google Ads API base endpoint. Intended for testing.");

    public static final Option<String> OAUTH_ENDPOINT =
            Options.key("oauth_endpoint")
                    .stringType()
                    .defaultValue("https://oauth2.googleapis.com")
                    .withDescription("Google OAuth2 token endpoint base. Intended for testing.");
}
