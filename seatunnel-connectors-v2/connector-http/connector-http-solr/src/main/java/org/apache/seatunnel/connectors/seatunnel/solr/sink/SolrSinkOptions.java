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

package org.apache.seatunnel.connectors.seatunnel.solr.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.Map;

/**
 * Defines options for existing-core HTTP updates, explicit delete permission and commit boundaries.
 * No option creates a collection, changes its schema or enables automatic request replay.
 */
public final class SolrSinkOptions {
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Trusted Solr server HTTPS origin, before the /solr path.");
    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of an existing Solr core or SolrCloud collection.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("User name for optional HTTP Basic authentication.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for optional HTTP Basic authentication.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable HTTP for an explicitly trusted, isolated endpoint.");
    public static final Option<String> UNIQUE_KEY =
            Options.key("unique_key")
                    .stringType()
                    .defaultValue("id")
                    .withDescription("Target unique-key field, verified against the Solr schema.");
    public static final Option<Map<String, String>> FIELD_MAPPING =
            Options.key("field_mapping")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "Optional mapping from input columns to target Solr field names.");
    public static final Option<Boolean> ENABLE_DELETE =
            Options.key("enable_delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow DELETE rows to delete an individual document by unique key.");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum consecutive rows of one operation per HTTP request (1 to 500).");
    public static final Option<CommitMode> COMMIT_MODE =
            Options.key("commit_mode")
                    .enumType(CommitMode.class)
                    .defaultValue(CommitMode.HARD)
                    .withDescription(
                            "Solr commit requested after pending writes at a flush boundary: HARD, SOFT or NONE.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Absolute deadline for each HTTP request, in milliseconds (1 to 300000).");

    private SolrSinkOptions() {}

    /**
     * Selects the core-wide commit requested after acknowledged writes; NONE leaves visibility and
     * durability policy to Solr, and none of these modes creates an engine checkpoint transaction.
     */
    public enum CommitMode {
        HARD,
        SOFT,
        NONE
    }
}
