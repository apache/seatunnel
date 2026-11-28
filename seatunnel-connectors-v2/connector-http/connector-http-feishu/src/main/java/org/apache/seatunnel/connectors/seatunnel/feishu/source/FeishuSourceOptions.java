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

package org.apache.seatunnel.connectors.seatunnel.feishu.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;

/**
 * Validates source identifiers, a trusted API origin and finite page/retry budgets before I/O.
 * Serializable reader copies share these settings; webhook Sink options retain their own contract.
 */
final class FeishuSourceOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    // Empty continuation pages also consume this finite full-refresh walk budget.
    static final int MAX_SCAN_PAGES = 10_000;

    static final Option<String> APP_ID =
            Options.key("app_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom app ID from the Feishu or Lark Developer Console");
    static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom app secret used to obtain a tenant access token");
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bitable (Base) token");
    static final Option<String> TABLE_ID =
            Options.key("table_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bitable table ID");
    static final Option<String> API_BASE_URL =
            Options.key("api_base_url")
                    .stringType()
                    .defaultValue("https://open.feishu.cn")
                    .withDescription("Feishu or Lark Open Platform origin, without an API path");
    static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Allow a cleartext API origin for an isolated test server");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum records requested per page, from 1 to 500");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(MAX_SCAN_PAGES)
                    .withDescription("Maximum pages in one full scan, from 1 to 10000");
    static final Option<Integer> PAGE_TIMEOUT_MS =
            Options.key("page_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Total page budget including authentication, requests and retries, from 1000 to 300000 ms");
    static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum rate-limit and server-error retries per request, from 0 to 5");

    // Validated configuration supplies authentication and the origin/app/table scope. Reader
    // snapshots store only that scope digest and progress, not authentication credentials.
    final String appId;
    final String clientSecret;
    final URI tokenUri;
    final URI recordsUri;
    final int pageSize;
    final int maxPages;
    final int pageTimeoutMs;
    final int maxRetries;

    /**
     * Builds credential-bearing API paths only from validated identifiers and one trusted origin.
     * Protocol budgets are finite before the reader creates transport or submits a request.
     */
    FeishuSourceOptions(ReadonlyConfig config) {
        appId = requiredText(config, APP_ID);
        clientSecret = requiredText(config, CLIENT_SECRET);
        String appToken = pathIdentifier(config, TOKEN);
        String tableId = pathIdentifier(config, TABLE_ID);
        URI origin = origin(config.get(API_BASE_URL), config.get(ALLOW_INSECURE_HTTP));
        tokenUri = origin.resolve("/open-apis/auth/v3/tenant_access_token/internal");
        recordsUri =
                origin.resolve(
                        "/open-apis/bitable/v1/apps/"
                                + appToken
                                + "/tables/"
                                + tableId
                                + "/records/search");
        pageSize = integer(config, PAGE_SIZE, 1, 500);
        maxPages = integer(config, MAX_PAGES, 1, MAX_SCAN_PAGES);
        pageTimeoutMs = integer(config, PAGE_TIMEOUT_MS, 1000, 300000);
        maxRetries = integer(config, MAX_RETRIES, 0, 5);
    }

    /**
     * Keeps authentication and record requests on an origin without embedded credentials or paths.
     * Plain HTTP requires the explicit isolated-provider option; HTTPS remains the default.
     */
    private static URI origin(String value, boolean allowInsecureHttp) {
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Feishu option 'api_base_url' must be a valid origin");
        }
        if (!("https".equalsIgnoreCase(uri.getScheme())
                        || (allowInsecureHttp && "http".equalsIgnoreCase(uri.getScheme())))
                || uri.getHost() == null
                || uri.getPort() < -1
                || uri.getPort() == 0
                || uri.getPort() > 65535
                || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null
                || !("".equals(uri.getRawPath()) || "/".equals(uri.getRawPath()))) {
            throw new IllegalArgumentException(
                    "Feishu option 'api_base_url' must be an HTTPS origin without user information, path, query or fragment");
        }
        return uri;
    }

    /**
     * Restricts Base/table identifiers before inserting them into credential-bearing API paths.
     * Validation errors name the option without echoing its supplied value.
     */
    private static String pathIdentifier(ReadonlyConfig config, Option<String> option) {
        String value = requiredText(config, option);
        if (!value.matches("[A-Za-z0-9_-]{1,256}")) {
            throw new IllegalArgumentException(
                    "Feishu option '" + option.key() + "' must be a valid Bitable identifier");
        }
        return value;
    }

    /**
     * Rejects missing, control-bearing or excessive credential/config text before transport setup.
     * Only option names appear in failures so private values cannot enter construction diagnostics.
     */
    private static String requiredText(ReadonlyConfig config, Option<String> option) {
        String value = config.get(option);
        if (value == null
                || value.trim().isEmpty()
                || value.length() > 4096
                || value.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException(
                    "Feishu option '"
                            + option.key()
                            + "' must be non-blank and at most 4096 characters");
        }
        return value;
    }

    private static int integer(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "Feishu option '" + option.key() + "' must be between " + min + " and " + max);
        }
        return value;
    }
}
