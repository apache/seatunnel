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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.util.Arrays;
import java.util.Collections;

/** Tests the security and compatibility boundaries of REST HOCON configuration resolution. */
public class RestUtilTest {

    /**
     * Verifies that only allowlisted environment variables are resolved while normal HOCON
     * references continue to work.
     */
    @Test
    @SetEnvironmentVariable(key = "SEATUNNEL_REST_ALLOWED_ENV", value = "allowed-value")
    @SetEnvironmentVariable(key = "SEATUNNEL_REST_DENIED_ENV", value = "denied-value")
    public void testBuildHoconConfigResolvesOnlyAllowlistedEnvironmentVariables() {
        Config config =
                RestUtil.buildHoconConfig(
                        "base = \"internal-value\"\n"
                                + "internal = ${base}\n"
                                + "allowed = ${SEATUNNEL_REST_ALLOWED_ENV}\n"
                                + "denied = ${SEATUNNEL_REST_DENIED_ENV}",
                        Collections.singletonList("SEATUNNEL_REST_ALLOWED_ENV"));

        Assertions.assertEquals("internal-value", config.getString("internal"));
        Assertions.assertEquals("allowed-value", config.getString("allowed"));
        Assertions.assertFalse(config.isResolved());
        Assertions.assertEquals(
                "${SEATUNNEL_REST_DENIED_ENV}",
                config.root().get("denied").render(ConfigRenderOptions.concise()));
    }

    /** Verifies that JVM system properties are never exposed to REST HOCON requests. */
    @Test
    @SetSystemProperty(key = "SEATUNNEL_REST_SYSTEM_PROPERTY", value = "system-secret")
    public void testBuildHoconConfigDoesNotResolveSystemProperties() {
        Config config =
                RestUtil.buildHoconConfig(
                        "value = ${SEATUNNEL_REST_SYSTEM_PROPERTY}", Collections.emptyList());

        Assertions.assertFalse(config.isResolved());
        Assertions.assertEquals(
                "${SEATUNNEL_REST_SYSTEM_PROPERTY}",
                config.root().get("value").render(ConfigRenderOptions.concise()));
    }

    /**
     * Verifies optional missing environment variables and connector placeholders keep their
     * established semantics when allowlist resolution is followed by ConfigShade decryption.
     */
    @Test
    @SetEnvironmentVariable(
            key = "SEATUNNEL_REST_ENCRYPTED_PASSWORD",
            value = "c2VhdHVubmVsX3Bhc3N3b3Jk")
    public void testBuildHoconConfigPreservesOptionalAndConnectorPlaceholderSemantics() {
        Config config =
                RestUtil.buildHoconConfig(
                        "optional = ${?SEATUNNEL_REST_MISSING_ENV}\n"
                                + "env {\n"
                                + "  job.mode = \"BATCH\"\n"
                                + "  shade.identifier = \"base64\"\n"
                                + "}\n"
                                + "source = [{\n"
                                + "  plugin_name = \"FakeSource\"\n"
                                + "  password = ${SEATUNNEL_REST_ENCRYPTED_PASSWORD}\n"
                                + "  string.template = \"${table_name}\"\n"
                                + "}]\n"
                                + "sink = [{ plugin_name = \"Console\" }]",
                        Arrays.asList(
                                "SEATUNNEL_REST_MISSING_ENV", "SEATUNNEL_REST_ENCRYPTED_PASSWORD"));

        Config decryptedConfig = ConfigShadeUtils.decryptConfig(config);

        Assertions.assertFalse(decryptedConfig.hasPath("optional"));
        Assertions.assertEquals(
                "${table_name}",
                decryptedConfig.getConfigList("source").get(0).getString("string.template"));
        Assertions.assertEquals(
                "seatunnel_password",
                decryptedConfig.getConfigList("source").get(0).getString("password"));
    }
}
