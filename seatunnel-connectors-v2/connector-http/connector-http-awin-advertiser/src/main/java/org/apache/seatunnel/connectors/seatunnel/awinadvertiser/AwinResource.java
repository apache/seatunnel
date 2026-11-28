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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

import java.util.Arrays;

/**
 * Provider routes and reporting dimensions for the advertiser API.
 *
 * <p>The account-wide operation is isolated from the five advertiser-scoped data requests.
 */
enum AwinResource {
    CAMPAIGN("campaign_performance", "/reports/campaign", true),
    TRANSACTIONS("transactions", "/transactions/", true),
    PUBLISHER_REPORT("publisher_performance", "/reports/publisher", true),
    CREATIVE("creative_performance", "/reports/creative", true),
    PUBLISHERS("publishers", "/publishers", false),
    ACCOUNTS("accounts", "/accounts", false);

    final String key;
    final String route;
    final boolean dated;

    AwinResource(String key, String route, boolean dated) {
        this.key = key;
        this.route = route;
        this.dated = dated;
    }

    static AwinResource select(String key) {
        return Arrays.stream(values())
                .filter(value -> value.key.equals(key))
                .findFirst()
                .orElseThrow(
                        () ->
                                AwinSettings.invalid(
                                        "resources", "contains an unsupported resource"));
    }
}
