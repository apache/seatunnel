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

package org.apache.seatunnel.connectors.seatunnel.shopify;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.shopify.source.config.ShopifySourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ShopifySourceParameterTest {

    @Test
    void buildWithConfigSetsAccessTokenHeaderAndPreservesExistingHeaders() {
        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("X-Custom-Header", "custom-value");

        Map<String, Object> options = new HashMap<>();
        options.put("url", "https://your-store.myshopify.com/admin/api/2024-01/products.json");
        options.put("access_token", "shpat_example_token");
        options.put("headers", customHeaders);

        ShopifySourceParameter parameter = new ShopifySourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(options));

        Map<String, String> headers = parameter.getHeaders();
        // the Shopify Admin API access token is sent in the X-Shopify-Access-Token header
        Assertions.assertEquals("shpat_example_token", headers.get("X-Shopify-Access-Token"));
        // requests ask for a JSON response
        Assertions.assertEquals("application/json", headers.get("Accept"));
        // existing custom headers are preserved
        Assertions.assertEquals("custom-value", headers.get("X-Custom-Header"));
    }
}
