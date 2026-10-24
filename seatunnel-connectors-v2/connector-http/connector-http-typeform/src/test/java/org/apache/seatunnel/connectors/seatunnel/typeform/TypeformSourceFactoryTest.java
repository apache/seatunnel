/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.typeform;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.typeform.config.TypeformSourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TypeformSourceFactoryTest {

    private static Map<String, Object> baseConfig() {
        Map<String, Object> m = new HashMap<>();
        m.put("api_key", "tfp_test-typeform-token-123");
        m.put("form_id", "lT9Z2j");
        return m;
    }

    private static TypeformSourceParameter build(Map<String, Object> m) {
        TypeformSourceParameter parameter = new TypeformSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(m));
        return parameter;
    }

    private static void assertRejected(Map<String, Object> m, String optionName) {
        Exception e = Assertions.assertThrows(IllegalArgumentException.class, () -> build(m));
        Assertions.assertTrue(e.getMessage().contains(optionName), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("tfp_test-typeform-token-123"));
    }

    @Test
    public void testFactoryIdentifier() {
        Assertions.assertEquals("Typeform", new TypeformSourceFactory().factoryIdentifier());
        Assertions.assertEquals(
                "Typeform",
                new TypeformSource(ReadonlyConfig.fromMap(baseConfig())).getPluginName());
    }

    @Test
    public void testParameterBuildingAndHeaders() {
        TypeformSourceParameter parameter = build(baseConfig());

        Assertions.assertEquals(
                "Bearer tfp_test-typeform-token-123", parameter.getHeaders().get("Authorization"));
        Assertions.assertEquals(HttpRequestMethod.GET, parameter.getMethod());
        Assertions.assertEquals(
                "https://api.typeform.com/forms/lT9Z2j/responses", parameter.getUrl());
        Assertions.assertEquals("100", parameter.getParams().get("page_size"));
        Assertions.assertEquals(1, parameter.getParams().size(), "no filters unless set");
        Assertions.assertFalse(parameter.getParams().containsValue("tfp_test-typeform-token-123"));
        Assertions.assertEquals(3, parameter.getRetry());
    }

    @Test
    public void testEuBaseUrlPageSizeAndFilters() {
        Map<String, Object> m = baseConfig();
        m.put("base_url", "https://api.eu.typeform.com/");
        m.put("page_size", 1000);
        m.put("since", "2026-01-01T00:00:00");
        m.put("until", "1767225600");
        m.put("response_type", "partial,completed");
        TypeformSourceParameter parameter = build(m);

        Assertions.assertEquals(
                "https://api.eu.typeform.com/forms/lT9Z2j/responses", parameter.getUrl());
        Assertions.assertEquals("1000", parameter.getParams().get("page_size"));
        Assertions.assertEquals("2026-01-01T00:00:00", parameter.getParams().get("since"));
        Assertions.assertEquals("1767225600", parameter.getParams().get("until"));
        Assertions.assertEquals("partial,completed", parameter.getParams().get("response_type"));
    }

    @Test
    public void testBlankFilterIsNotSent() {
        Map<String, Object> m = baseConfig();
        m.put("since", "  ");
        Assertions.assertFalse(build(m).getParams().containsKey("since"));
    }

    @Test
    public void testBlankApiKeyRejected() {
        Map<String, Object> m = baseConfig();
        m.put("api_key", "   ");
        assertRejected(m, "api_key");
    }

    @Test
    public void testBlankFormIdRejected() {
        Map<String, Object> m = baseConfig();
        m.put("form_id", "");
        assertRejected(m, "form_id");
    }

    @Test
    public void testFormIdWithPathCharactersRejected() {
        for (String bad : new String[] {"../me", "abc/def", "abc?x=1", "a b"}) {
            Map<String, Object> m = baseConfig();
            m.put("form_id", bad);
            assertRejected(m, "form_id");
        }
    }

    @Test
    public void testPageSizeOutOfRangeRejected() {
        for (int bad : new int[] {0, 1001}) {
            Map<String, Object> m = baseConfig();
            m.put("page_size", bad);
            assertRejected(m, "page_size");
        }
    }

    @Test
    public void testNegativeBackoffRejected() {
        Map<String, Object> m = baseConfig();
        m.put("retry_backoff_multiplier_ms", -1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> build(m));
    }

    @Test
    public void testCustomBackoffHonoredWithoutExplicitRetry() {
        Map<String, Object> m = baseConfig();
        m.put("retry_backoff_multiplier_ms", 2000);
        m.put("retry_backoff_max_ms", 3000);
        TypeformSourceParameter parameter = build(m);

        Assertions.assertEquals(3, parameter.getRetry());
        Assertions.assertEquals(2000, parameter.getRetryBackoffMultiplierMillis());
        Assertions.assertEquals(3000, parameter.getRetryBackoffMaxMillis());
    }

    @Test
    public void testDefaultBackoffWhenNothingConfigured() {
        TypeformSourceParameter parameter = build(baseConfig());

        Assertions.assertEquals(100, parameter.getRetryBackoffMultiplierMillis());
        Assertions.assertEquals(10000, parameter.getRetryBackoffMaxMillis());
    }
}
