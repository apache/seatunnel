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
package org.apache.seatunnel.connectors.seatunnel.discourse;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

final class DiscourseOptions {
    static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Discourse site URL, optionally with its deployment subpath.");
    static final Option<Resource> RESOURCE =
            Options.key("resource")
                    .enumType(Resource.class)
                    .noDefaultValue()
                    .withDescription("Metadata resource to read.");
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional Discourse API key, paired with username; requires HTTPS.");
    static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional Discourse API username, paired with token; requires HTTPS.");
    static final Option<Boolean> INCLUDE_SUBCATEGORIES =
            Options.key("include_subcategories")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Include subcategories in the CATEGORIES response.");
    static final Option<Integer> TOPIC_LIMIT =
            Options.key("topic_limit")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "Number of topics requested for LATEST_TOPICS, from 1 to 100.");

    enum Resource {
        CATEGORIES("categories", "category_list"),
        LATEST_TOPICS("topics", "topic_list");

        final String rows;
        final String envelope;

        Resource(String rows, String envelope) {
            this.rows = rows;
            this.envelope = envelope;
        }
    }

    private DiscourseOptions() {}
}
