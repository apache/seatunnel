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

package org.apache.seatunnel.connectors.seatunnel.salesforce;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.salesforce.source.SalesforceSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SalesforceSourceFactoryTest {

    private static final SalesforceSourceFactory FACTORY = new SalesforceSourceFactory();

    @Test
    void testOptionRuleIsNotNull() {
        Assertions.assertNotNull(FACTORY.optionRule());
    }

    @Test
    void testFactoryIdentifier() {
        Assertions.assertEquals("Salesforce", FACTORY.factoryIdentifier());
    }

    @Test
    void testSingleObjectModeValid() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("object_name", "Account");

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testSingleObjectModeWithOptionalParamsValid() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("object_name", "Account");
        config.put("filter", "AnnualRevenue > 1000000");
        config.put("api_version", "v58.0");
        config.put("max_retries", 5);
        config.put("request_timeout_ms", 30000);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testMultiObjectModeValid() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("tables_configs", multiObjectTablesConfigs());

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testBothObjectNameAndTablesConfigsThrows() {
        Map<String, Object> config = requiredAuthConfig();
        config.put("object_name", "Account");
        config.put("tables_configs", multiObjectTablesConfigs());

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testNeitherObjectNameNorTablesConfigsThrows() {
        Map<String, Object> config = requiredAuthConfig();

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testMissingClientIdThrows() {
        Map<String, Object> config = requiredAuthConfig();
        config.remove("client_id");
        config.put("object_name", "Account");

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testMissingInstanceUrlThrows() {
        Map<String, Object> config = requiredAuthConfig();
        config.remove("instance_url");
        config.put("object_name", "Account");

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    @Test
    void testMissingUsernameThrows() {
        Map<String, Object> config = requiredAuthConfig();
        config.remove("username");
        config.put("object_name", "Account");

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                .validate(FACTORY.optionRule()));
    }

    private static Map<String, Object> requiredAuthConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("client_id", "test_client_id");
        config.put("client_secret", "test_client_secret");
        config.put("username", "user@company.com");
        config.put("password", "test_password");
        config.put("instance_url", "https://test.salesforce.com");
        return config;
    }

    private static List<Map<String, Object>> multiObjectTablesConfigs() {
        Map<String, Object> account = new HashMap<>();
        account.put("table_path", "salesforce.Account");

        Map<String, Object> contact = new HashMap<>();
        contact.put("table_path", "salesforce.Contact");
        contact.put("filter", "IsDeleted = false");

        return Arrays.asList(account, contact);
    }
}
