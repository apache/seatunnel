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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.source.FacebookAdsSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FacebookAdsSourceFactoryTest {

    private static final FacebookAdsSourceFactory FACTORY = new FacebookAdsSourceFactory();

    @Test
    void testOptionRuleIsNotNull() {
        Assertions.assertNotNull(FACTORY.optionRule());
    }

    @Test
    void testFactoryIdentifier() {
        Assertions.assertEquals("FacebookAds", FACTORY.factoryIdentifier());
    }

    @Test
    void testResourceModeValid() {
        Map<String, Object> config = requiredConfig();
        config.put("resource", "campaigns");
        config.put("fields", Arrays.asList("id", "name", "status"));

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testResourceModeWithOptionalParamsValid() {
        Map<String, Object> config = requiredConfig();
        config.put("resource", "insights");
        config.put("fields", Arrays.asList("campaign_id", "impressions", "spend"));
        config.put(
                "filtering",
                "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}]");
        Map<String, String> params = new HashMap<>();
        params.put("date_preset", "last_30d");
        params.put("level", "campaign");
        config.put("params", params);
        config.put("api_version", "v23.0");
        config.put("max_retries", 5);
        config.put("request_timeout_ms", 30000);
        config.put("retry_backoff_ms", 500L);
        config.put("page_size", 100);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testTablesConfigsModeValid() {
        Map<String, Object> config = requiredConfig();
        config.put("tables_configs", multiTableConfigs());

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testResourceAndTablesConfigsTogetherThrows() {
        Map<String, Object> config = requiredConfig();
        config.put("resource", "campaigns");
        config.put("tables_configs", multiTableConfigs());

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testNoModeSelectedThrows() {
        Map<String, Object> config = requiredConfig();

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testMissingAccessTokenThrows() {
        assertMissingRequiredThrows("access_token");
    }

    @Test
    void testMissingAdAccountIdThrows() {
        assertMissingRequiredThrows("ad_account_id");
    }

    private static void assertMissingRequiredThrows(String key) {
        Map<String, Object> config = requiredConfig();
        config.remove(key);
        config.put("resource", "campaigns");
        config.put("fields", Arrays.asList("id"));

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    private static Map<String, Object> requiredConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("access_token", "test_access_token");
        config.put("ad_account_id", "1234567890");
        return config;
    }

    private static List<Map<String, Object>> multiTableConfigs() {
        Map<String, Object> campaigns = new HashMap<>();
        campaigns.put("table_path", "facebook_ads.campaigns");
        campaigns.put("fields", Arrays.asList("id", "name"));

        Map<String, Object> insights = new HashMap<>();
        insights.put("table_path", "facebook_ads.insights");
        insights.put("fields", Arrays.asList("campaign_id", "impressions"));
        insights.put("ad_account_id", "2345678901");

        return Arrays.asList(campaigns, insights);
    }
}
