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

package org.apache.seatunnel.connectors.seatunnel.shopify.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

/**
 * The options this connector adds to the HTTP source's own: the Admin API access token, and the
 * header names used to send it.
 */
public class ShopifySourceOptions extends HttpCommonOptions {
    public static final String ACCESS_TOKEN_HEADER = "X-Shopify-Access-Token";
    public static final String ACCEPT = "Accept";
    public static final String APPLICATION_JSON = "application/json";

    public static final Option<String> ACCESS_TOKEN =
            Options.key("access_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Shopify Admin API access token");
}
