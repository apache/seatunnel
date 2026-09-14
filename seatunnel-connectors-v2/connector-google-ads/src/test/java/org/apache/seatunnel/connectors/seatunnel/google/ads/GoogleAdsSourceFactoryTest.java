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

package org.apache.seatunnel.connectors.seatunnel.google.ads;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.google.ads.source.GoogleAdsSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class GoogleAdsSourceFactoryTest {

    private static final GoogleAdsSourceFactory FACTORY = new GoogleAdsSourceFactory();

    @Test
    void testOptionRuleIsNotNull() {
        Assertions.assertNotNull(FACTORY.optionRule());
    }

    @Test
    void testFactoryIdentifier() {
        Assertions.assertEquals("GoogleAds", FACTORY.factoryIdentifier());
    }

    @Test
    void testResourceModeValid() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("resource", "campaign");
        config.put("fields", Arrays.asList("campaign.id", "campaign.name", "metrics.clicks"));

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testResourceModeWithOptionalParamsValid() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("resource", "campaign");
        config.put("fields", Arrays.asList("campaign.id"));
        config.put("filter", "segments.date DURING LAST_30_DAYS");
        config.put("login_customer_id", "9999999999");
        config.put("api_version", "v21");
        config.put("max_retries", 5);
        config.put("request_timeout_ms", 30000);
        config.put("retry_backoff_ms", 500L);
        config.put("page_size", 1000);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testQueryModeValid() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("query", "SELECT campaign.id, metrics.clicks FROM campaign");

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testTablesConfigsModeValid() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("tables_configs", multiTableConfigs());

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testResourceAndQueryTogetherThrows() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("resource", "campaign");
        config.put("query", "SELECT campaign.id FROM campaign");

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testResourceAndTablesConfigsTogetherThrows() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("resource", "campaign");
        config.put("tables_configs", multiTableConfigs());

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testQueryAndTablesConfigsTogetherThrows() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("query", "SELECT campaign.id FROM campaign");
        config.put("tables_configs", multiTableConfigs());

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testNoModeSelectedThrows() {
        Map<String, Object> config = requiredAuthConfig();

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testMissingDeveloperTokenThrows() {
        assertMissingRequiredThrows("developer_token");
    }

    @Test
    void testMissingClientIdThrows() {
        assertMissingRequiredThrows("client_id");
    }

    @Test
    void testMissingClientSecretThrows() {
        assertMissingRequiredThrows("client_secret");
    }

    @Test
    void testMissingRefreshTokenThrows() {
        assertMissingRequiredThrows("refresh_token");
    }

    @Test
    void testMissingCustomerIdThrows() {
        assertMissingRequiredThrows("customer_id");
    }

    private static void assertMissingRequiredThrows(String key) {
        Map<String, Object> config = requiredAuthConfig();
        config.remove(key);
        config.put("query", "SELECT campaign.id FROM campaign");

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    private static Map<String, Object> requiredAuthConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("developer_token", "test_developer_token");
        config.put("client_id", "test_client_id");
        config.put("client_secret", "test_client_secret");
        config.put("refresh_token", "test_refresh_token");
        config.put("customer_id", "1234567890");
        return config;
    }

    private static List<Map<String, Object>> multiTableConfigs() {
        Map<String, Object> campaign = new HashMap<>();
        campaign.put("table_path", "google_ads.campaign");
        campaign.put("fields", Arrays.asList("campaign.id", "campaign.name"));

        Map<String, Object> adGroup = new HashMap<>();
        adGroup.put("table_path", "google_ads.ad_group");
        adGroup.put("query", "SELECT ad_group.id, metrics.clicks FROM ad_group");
        adGroup.put("customer_id", "2345678901");

        return Arrays.asList(campaign, adGroup);
    }
}
