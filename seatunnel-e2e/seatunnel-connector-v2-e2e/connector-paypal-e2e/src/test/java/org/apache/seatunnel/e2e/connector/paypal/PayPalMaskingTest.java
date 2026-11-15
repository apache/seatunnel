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

package org.apache.seatunnel.e2e.connector.paypal;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PayPalMaskingTest {
    @Test
    void nativeParsedConfigMasksClientSecretWithoutChangingSourceConfig() {
        Config config =
                ConfigFactory.parseString(
                        "source { PayPal { client_id = \"example-id\", client_secret = \"DO-NOT-LOG-PAYPAL\" } }");
        Object sources = config.root().unwrapped().get("source");
        assertTrue(sources instanceof List);
        assertEquals(1, ((List<?>) sources).size());
        assertTrue(((List<?>) sources).get(0) instanceof Map);
        assertTrue(
                ConfigShadeUtils.getLogDesensitizationOptions(config).contains("client_secret"),
                "Native mask rules missing from "
                        + ConfigShadeUtils.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation());
        Map<String, Object> masked =
                ConfigBuilder.configDesensitization(
                        config.root().unwrapped(),
                        ConfigShadeUtils.getLogDesensitizationOptions(config));
        assertFalse(masked.toString().contains("DO-NOT-LOG-PAYPAL"));
        assertTrue(
                config.getConfigList("source")
                        .get(0)
                        .getString("client_secret")
                        .equals("DO-NOT-LOG-PAYPAL"));
    }
}
