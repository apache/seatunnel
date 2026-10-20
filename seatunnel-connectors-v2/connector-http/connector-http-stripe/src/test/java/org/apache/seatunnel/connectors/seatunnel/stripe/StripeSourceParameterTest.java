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

package org.apache.seatunnel.connectors.seatunnel.stripe;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.stripe.source.StripeSource;
import org.apache.seatunnel.connectors.seatunnel.stripe.source.StripeSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.stripe.source.config.StripeSourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class StripeSourceParameterTest {

    @Test
    void buildsPaymentIntentsRequestWithoutExposingSecret() {
        Map<String, Object> options = baseOptions();
        options.put("api_base_url", "https://stripe.example/");
        options.put("api_version", "2025-04-30.basil");
        options.put("page_size", 25);
        options.put("created_gte", 100L);
        options.put("created_lt", 200L);

        StripeSourceParameter parameter = new StripeSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(options));

        Assertions.assertEquals("https://stripe.example/v1/payment_intents", parameter.getUrl());
        Assertions.assertEquals("get", parameter.getMethod().getMethod());
        Assertions.assertEquals(
                "Bearer sk_test_secret", parameter.getHeaders().get("Authorization"));
        Assertions.assertEquals("2025-04-30.basil", parameter.getHeaders().get("Stripe-Version"));
        Assertions.assertEquals("25", parameter.getParams().get("limit"));
        Assertions.assertEquals("100", parameter.getParams().get("created[gte]"));
        Assertions.assertEquals("200", parameter.getParams().get("created[lt]"));
        Assertions.assertFalse(parameter.toString().contains("sk_test_secret"));
    }

    @Test
    void validatesPageSizeAndTimeRange() {
        Map<String, Object> invalidPageSizeOptions = baseOptions();
        invalidPageSizeOptions.put("page_size", 101);
        IllegalArgumentException pageSizeError =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> buildParameter(invalidPageSizeOptions));
        Assertions.assertTrue(pageSizeError.getMessage().contains("page_size"));

        Map<String, Object> invalidTimeRangeOptions = baseOptions();
        invalidTimeRangeOptions.put("created_gte", 200L);
        invalidTimeRangeOptions.put("created_lt", 200L);
        IllegalArgumentException rangeError =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> buildParameter(invalidTimeRangeOptions));
        Assertions.assertTrue(rangeError.getMessage().contains("created_gte"));
    }

    @Test
    void factoryDeclaresRequiredSecretKey() {
        Assertions.assertNotNull(new StripeSourceFactory().optionRule());
        Assertions.assertEquals("Stripe", new StripeSourceFactory().factoryIdentifier());
    }

    @Test
    void exposesBoundedSingleColumnJsonContract() {
        StripeSource source = new StripeSource(ReadonlyConfig.fromMap(baseOptions()));
        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH));

        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        CatalogTable table = source.getProducedCatalogTables().get(0);
        Assertions.assertArrayEquals(
                new String[] {"content"}, table.getSeaTunnelRowType().getFieldNames());
        Assertions.assertEquals(BasicType.STRING_TYPE, table.getSeaTunnelRowType().getFieldType(0));

        source.setJobContext(new JobContext().setJobMode(JobMode.STREAMING));
        Assertions.assertThrows(UnsupportedOperationException.class, source::getBoundedness);
    }

    private static StripeSourceParameter buildParameter(Map<String, Object> options) {
        StripeSourceParameter parameter = new StripeSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(options));
        return parameter;
    }

    private static Map<String, Object> baseOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("secret_key", "sk_test_secret");
        return options;
    }
}
