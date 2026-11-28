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
package org.apache.seatunnel.connectors.seatunnel.azuretable;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;
import java.util.Map;

/**
 * Options for an Azure Table Storage bounded account scan.
 *
 * <p>Endpoints are derived from the account name and the supported cloud suffixes.
 */
public final class AzureTableSourceOptions {
    static final Option<String> ACCOUNT = text("account_name", "Azure Storage account name.");
    static final Option<String> SECRET =
            text("secret_key", "Base64 Azure Storage account access key.");
    static final Option<String> SUFFIX =
            Options.key("endpoint_suffix")
                    .stringType()
                    .defaultValue("core.windows.net")
                    .withDescription("DNS suffix for the Azure cloud.");
    static final Option<String> CERTIFICATE =
            text("tls_trust_certificate", "Optional X.509 trust anchor in PEM or base64 DER form.");
    static final Option<List<String>> TABLES =
            Options.key("tables")
                    .listType(String.class)
                    .defaultValue(List.of())
                    .withDescription("Table names in scan order. Empty discovers all tables.");
    static final Option<Map<String, String>> PARTITION_KEYS =
            Options.key("start_after_partition_keys")
                    .mapType()
                    .defaultValue(Map.of())
                    .withDescription("Exclusive starting partition keys, by table.");
    static final Option<Map<String, String>> ROW_KEYS =
            Options.key("start_after_row_keys")
                    .mapType()
                    .defaultValue(Map.of())
                    .withDescription("Exclusive starting row keys, by the same tables.");
    static final Option<Integer> PAGE_SIZE =
            number("page_size", 1000, "Requested page size from 1 to 1000.");
    static final Option<Integer> MAX_PAGES =
            number(
                    "max_pages_per_collection",
                    10000,
                    "Page limit for discovery and each entity table individually.");
    static final Option<Integer> MAX_TABLES =
            number("max_tables", 10000, "Maximum selected or discovered table names.");
    static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connect timeout.");
    static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Deadline across attempts for one logical read.");
    static final Option<Integer> INTERVAL =
            number("min_request_interval_ms", 0, "Minimum interval between HTTPS attempts.");
    static final Option<Integer> RETRIES =
            number("max_retries", 3, "Additional attempts for retryable failures.");
    static final Option<Integer> BODY_LIMIT =
            number("max_response_bytes", 4194304, "Maximum encoded and decoded response bytes.");

    private AzureTableSourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int fallback, String description) {
        return Options.key(name).intType().defaultValue(fallback).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(ACCOUNT, SECRET)
                .optional(
                        SUFFIX,
                        CERTIFICATE,
                        TABLES,
                        PARTITION_KEYS,
                        ROW_KEYS,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_TABLES,
                        CONNECT_TIMEOUT,
                        REQUEST_TIMEOUT,
                        OPERATION_TIMEOUT,
                        INTERVAL,
                        RETRIES,
                        BODY_LIMIT)
                .build();
    }
}
