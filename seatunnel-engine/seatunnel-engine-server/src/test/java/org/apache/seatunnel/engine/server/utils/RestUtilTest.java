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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;

import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.util.Arrays;
import java.util.Collections;

/** Tests the security and compatibility boundaries of REST HOCON configuration resolution. */
public class RestUtilTest {

    /** Environment variable whose value collides with a temporary placeholder token. */
    private static final String COLLISION_ENV = "SEATUNNEL_REST_COLLISION_ENV";

    /** User-provided value that must not be rewritten during placeholder restoration. */
    private static final String COLLISION_ENV_VALUE = "__SEATUNNEL_REST_SYSTEM_PLACEHOLDER_1__";

    /**
     * Verifies that only allowlisted environment variables are resolved while normal HOCON
     * references continue to work.
     */
    @Test
    @SetEnvironmentVariable(key = "SEATUNNEL_REST_ALLOWED_ENV", value = "allowed-value")
    public void testBuildHoconConfigResolvesAllowlistedEnvironmentVariables() {
        Config config =
                RestUtil.buildHoconConfig(
                        "base = \"internal-value\"\n"
                                + "internal = ${base}\n"
                                + "allowed = ${SEATUNNEL_REST_ALLOWED_ENV}",
                        Collections.singletonList("SEATUNNEL_REST_ALLOWED_ENV"));

        Assertions.assertEquals("internal-value", config.getString("internal"));
        Assertions.assertEquals("allowed-value", config.getString("allowed"));
        Assertions.assertTrue(config.isResolved());
    }

    /**
     * Verifies that environment variables outside the REST allowlist remain unresolved and their
     * values are not exposed.
     */
    @Test
    @SetEnvironmentVariable(key = "SEATUNNEL_REST_DENIED_ENV", value = "denied-value")
    public void testBuildHoconConfigDoesNotResolveEnvironmentVariablesOutsideAllowlist() {
        Config config =
                RestUtil.buildHoconConfig(
                        "denied = ${SEATUNNEL_REST_DENIED_ENV}", Collections.emptyList());

        Assertions.assertFalse(config.isResolved());
        ConfigException.NotResolved exception =
                Assertions.assertThrows(
                        ConfigException.NotResolved.class, () -> config.getString("denied"));
        Assertions.assertFalse(exception.getMessage().contains("denied-value"));
    }

    /** Verifies that JVM system properties are never exposed to REST HOCON requests. */
    @Test
    @SetSystemProperty(key = "SEATUNNEL_REST_SYSTEM_PROPERTY", value = "system-secret")
    public void testBuildHoconConfigDoesNotResolveSystemProperties() {
        Config config =
                RestUtil.buildHoconConfig(
                        "value = ${SEATUNNEL_REST_SYSTEM_PROPERTY}", Collections.emptyList());

        Assertions.assertFalse(config.isResolved());
        ConfigException.NotResolved exception =
                Assertions.assertThrows(
                        ConfigException.NotResolved.class, () -> config.getString("value"));
        Assertions.assertFalse(exception.getMessage().contains("system-secret"));
    }

    /**
     * Verifies optional missing environment variables and connector placeholders keep their
     * established semantics when allowlist resolution is followed by ConfigShade decryption.
     */
    @Test
    @SetEnvironmentVariable(
            key = "SEATUNNEL_REST_ENCRYPTED_PASSWORD",
            value = "c2VhdHVubmVsX3Bhc3N3b3Jk")
    @SetEnvironmentVariable(key = COLLISION_ENV, value = COLLISION_ENV_VALUE)
    public void testBuildHoconConfigPreservesOptionalAndConnectorPlaceholderSemantics() {
        Config config =
                RestUtil.buildHoconConfig(
                        "optional = ${?SEATUNNEL_REST_MISSING_ENV}\n"
                                + "env {\n"
                                + "  job.mode = \"BATCH\"\n"
                                + "  shade.identifier = \"base64\"\n"
                                + "}\n"
                                + "token.prefix = \"__SEATUNNEL_REST_SYSTEM_\"\n"
                                + "token.suffix = \"PLACEHOLDER_0__\"\n"
                                + "source = [{\n"
                                + "  plugin_name = \"FakeSource\"\n"
                                + "  password = ${SEATUNNEL_REST_ENCRYPTED_PASSWORD}\n"
                                + "  literal.collision = \"__SEATUNNEL_REST_SYSTEM_PLACEHOLDER_0__\"\n"
                                + "  env.collision = ${"
                                + COLLISION_ENV
                                + "}\n"
                                + "  resolved.collision = ${token.prefix}${token.suffix}\n"
                                + "  string.template = ${table_name}\n"
                                + "  default.template = ${table_name:default_table}\n"
                                + "}]\n"
                                + "sink = [{ plugin_name = \"Console\" }]",
                        Arrays.asList(
                                "SEATUNNEL_REST_MISSING_ENV",
                                "SEATUNNEL_REST_ENCRYPTED_PASSWORD",
                                COLLISION_ENV));

        Config decryptedConfig = ConfigShadeUtils.decryptConfig(config);
        Config sourceConfig = decryptedConfig.getConfigList("source").get(0);

        Assertions.assertFalse(decryptedConfig.hasPath("optional"));
        Assertions.assertEquals(
                "__SEATUNNEL_REST_SYSTEM_PLACEHOLDER_0__",
                sourceConfig.getString("literal.collision"));
        Assertions.assertEquals(COLLISION_ENV_VALUE, sourceConfig.getString("env.collision"));
        Assertions.assertEquals(
                "__SEATUNNEL_REST_SYSTEM_PLACEHOLDER_0__",
                sourceConfig.getString("resolved.collision"));
        Assertions.assertEquals("${table_name}", sourceConfig.getString("string.template"));
        Assertions.assertEquals(
                "${table_name:default_table}", sourceConfig.getString("default.template"));
        Assertions.assertEquals("seatunnel_password", sourceConfig.getString("password"));
    }
}
