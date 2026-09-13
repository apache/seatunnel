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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsTableConfig;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Covers the config-resolution logic of {@link FacebookAdsSource}: mode dispatch, table_path
 * handling, field/resource/ad_account_id validation and the all-STRING schema. No HTTP server is
 * needed because the schema is built locally without network calls.
 */
class FacebookAdsSourceTest {

    private static final String AD_ACCOUNT_ID = "1234567890";

    @Test
    void resourceModeBuildsStringSchemaInFieldOrder() {
        Map<String, Object> map = baseConfig();
        map.put("resource", "campaigns");
        map.put("fields", Arrays.asList("id", "name", "status"));

        FacebookAdsSource source = buildSource(map);

        List<FacebookAdsTableConfig> configs = source.getTableConfigs();
        Assertions.assertEquals(1, configs.size());
        FacebookAdsTableConfig table = configs.get(0);
        Assertions.assertEquals("campaigns", table.getResource());
        Assertions.assertEquals(AD_ACCOUNT_ID, table.getAdAccountId());
        Assertions.assertEquals("facebook_ads.campaigns", table.getTableId());
        SeaTunnelRowType rowType = source.getProducedCatalogTables().get(0).getSeaTunnelRowType();
        Assertions.assertEquals(3, rowType.getTotalFields());
        Assertions.assertEquals("id", rowType.getFieldName(0));
        Assertions.assertEquals("name", rowType.getFieldName(1));
        Assertions.assertEquals("status", rowType.getFieldName(2));
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Assertions.assertEquals(SqlType.STRING, rowType.getFieldType(i).getSqlType());
        }
    }

    @Test
    void resourceModeRequiresNonEmptyFields() {
        Map<String, Object> map = baseConfig();
        map.put("resource", "campaigns");

        FacebookAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                FacebookAdsConnectorErrorCode.INVALID_CONFIG, ex.getSeaTunnelErrorCode());
        Assertions.assertTrue(ex.getMessage().contains("fields"));
    }

    @Test
    void missingAllModesIsRejected() {
        FacebookAdsConnectorException ex = assertBuildFails(baseConfig());
        Assertions.assertEquals(
                FacebookAdsConnectorErrorCode.INVALID_CONFIG, ex.getSeaTunnelErrorCode());
    }

    @Test
    void invalidFieldNamesAreRejectedWithOffendingName() {
        for (String bad : Arrays.asList("Campaign.Id", "id;drop", "bad field", "id,name")) {
            Map<String, Object> map = baseConfig();
            map.put("resource", "campaigns");
            map.put("fields", Arrays.asList(bad));

            FacebookAdsConnectorException ex = assertBuildFails(map);
            Assertions.assertEquals(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG, ex.getSeaTunnelErrorCode());
            Assertions.assertTrue(ex.getMessage().contains(bad), "message should name: " + bad);
        }
    }

    @Test
    void invalidResourceNamesAreRejected() {
        for (String bad : Arrays.asList("Campaigns", "camp/aigns", "insights?x=1")) {
            Map<String, Object> map = baseConfig();
            map.put("resource", bad);
            map.put("fields", Arrays.asList("id"));

            FacebookAdsConnectorException ex = assertBuildFails(map);
            Assertions.assertEquals(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG, ex.getSeaTunnelErrorCode());
            Assertions.assertTrue(ex.getMessage().contains(bad), "message should name: " + bad);
        }
    }

    @Test
    void reservedParamsAreRejected() {
        for (String reserved :
                Arrays.asList("fields", "limit", "after", "filtering", "access_token")) {
            Map<String, Object> map = baseConfig();
            map.put("resource", "campaigns");
            map.put("fields", Arrays.asList("id"));
            Map<String, String> params = new HashMap<>();
            params.put(reserved, "x");
            map.put("params", params);

            FacebookAdsConnectorException ex = assertBuildFails(map);
            Assertions.assertEquals(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG, ex.getSeaTunnelErrorCode());
            Assertions.assertTrue(
                    ex.getMessage().contains(reserved), "message should name: " + reserved);
        }
    }

    @Test
    void actPrefixOnAdAccountIdIsStripped() {
        Map<String, Object> map = baseConfig();
        map.put("ad_account_id", "act_" + AD_ACCOUNT_ID);
        map.put("resource", "campaigns");
        map.put("fields", Arrays.asList("id"));

        FacebookAdsTableConfig table = buildSource(map).getTableConfigs().get(0);
        Assertions.assertEquals(AD_ACCOUNT_ID, table.getAdAccountId());
    }

    @Test
    void invalidAdAccountIdIsRejected() {
        for (String bad : Arrays.asList("abc", "123abc", "act_")) {
            Map<String, Object> map = baseConfig();
            map.put("ad_account_id", bad);
            map.put("resource", "campaigns");
            map.put("fields", Arrays.asList("id"));

            FacebookAdsConnectorException ex = assertBuildFails(map);
            Assertions.assertEquals(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG,
                    ex.getSeaTunnelErrorCode(),
                    "ad_account_id should be rejected: " + bad);
        }
    }

    @Test
    void tablesConfigsBuildsMultipleTablesWithPerTableAdAccountIdFallback() {
        Map<String, Object> entryWithOverride = new HashMap<>();
        entryWithOverride.put("table_path", "facebook_ads.campaigns");
        entryWithOverride.put("fields", Arrays.asList("id", "name"));
        entryWithOverride.put("ad_account_id", "act_2222222222");
        Map<String, Object> entryWithFallback = new HashMap<>();
        entryWithFallback.put("table_path", "facebook_ads.insights");
        entryWithFallback.put("fields", Arrays.asList("campaign_id", "impressions", "spend"));

        Map<String, Object> map = baseConfig();
        map.put("tables_configs", Arrays.asList(entryWithOverride, entryWithFallback));

        List<FacebookAdsTableConfig> configs = buildSource(map).getTableConfigs();
        Assertions.assertEquals(2, configs.size());
        Assertions.assertEquals("facebook_ads.campaigns", configs.get(0).getTableId());
        Assertions.assertEquals("2222222222", configs.get(0).getAdAccountId());
        Assertions.assertEquals("facebook_ads.insights", configs.get(1).getTableId());
        Assertions.assertEquals(AD_ACCOUNT_ID, configs.get(1).getAdAccountId());
    }

    @Test
    void tablesConfigsRejectsEmptyList() {
        Map<String, Object> map = baseConfig();
        map.put("tables_configs", Collections.emptyList());

        FacebookAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                FacebookAdsConnectorErrorCode.INVALID_CONFIG, ex.getSeaTunnelErrorCode());
        Assertions.assertTrue(ex.getMessage().contains("tables_configs"));
    }

    @Test
    void tablesConfigsRejectsDuplicateTablePath() {
        Map<String, Object> entry1 = new HashMap<>();
        entry1.put("table_path", "facebook_ads.campaigns");
        entry1.put("fields", Arrays.asList("id"));
        Map<String, Object> entry2 = new HashMap<>();
        entry2.put("table_path", "facebook_ads.campaigns");
        entry2.put("fields", Arrays.asList("name"));

        Map<String, Object> map = baseConfig();
        map.put("tables_configs", Arrays.asList(entry1, entry2));

        FacebookAdsConnectorException ex = assertBuildFails(map);
        Assertions.assertEquals(
                FacebookAdsConnectorErrorCode.DUPLICATE_RESOURCE, ex.getSeaTunnelErrorCode());
    }

    @Test
    void tablesConfigsRejectsMalformedTablePath() {
        for (String badPath : Arrays.asList("campaigns", ".campaigns", "facebook_ads.")) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("table_path", badPath);
            entry.put("fields", Arrays.asList("id"));

            Map<String, Object> map = baseConfig();
            map.put("tables_configs", Arrays.asList(entry));

            FacebookAdsConnectorException ex = assertBuildFails(map);
            Assertions.assertEquals(
                    FacebookAdsConnectorErrorCode.INVALID_TABLE_PATH,
                    ex.getSeaTunnelErrorCode(),
                    "table_path should be rejected: " + badPath);
        }
    }

    private FacebookAdsConnectorException assertBuildFails(Map<String, Object> map) {
        return Assertions.assertThrows(FacebookAdsConnectorException.class, () -> buildSource(map));
    }

    private FacebookAdsSource buildSource(Map<String, Object> map) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(map);
        FacebookAdsParameters params = new FacebookAdsParameters();
        params.buildWithConfig(config);
        return new FacebookAdsSource(params, config);
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("access_token", "test-token");
        map.put("ad_account_id", AD_ACCOUNT_ID);
        return map;
    }
}
