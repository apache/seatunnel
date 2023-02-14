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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ConfigShade;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.apache.seatunnel.core.starter.utils.ConfigBuilder.CONFIG_RENDER_OPTIONS;

@Slf4j
public class ConfigShadeTest {

    @Test
    public void testParseConfig() {
        URL resource = ConfigShadeTest.class.getResource("/config.shade.conf");
        Assertions.assertNotNull(resource);
        String file = resource.getFile();
        Config config = ConfigBuilder.of(file);
        Config decryptConfig = ConfigShadeUtils.decryptConfig(config);
        Config fields =
                decryptConfig
                        .getConfigList("source")
                        .get(0)
                        .getConfig("schema")
                        .getConfig("fields");
        log.info("Schema fields: {}", fields.root().render(CONFIG_RENDER_OPTIONS));
        log.info("Decrypt config: {}", decryptConfig.root().render(CONFIG_RENDER_OPTIONS));
    }

    @Test
    public void testDecryptAndEncrypt() {
        String username = "seatunnel";
        String password = "seatunnel_password";
        String encryptUsername = ConfigShadeUtils.encryptOption("base64", username);
        String decryptUsername = ConfigShadeUtils.decryptOption("base64", encryptUsername);
        String encryptPassword = ConfigShadeUtils.encryptOption("base64", password);
        String decryptPassword = ConfigShadeUtils.decryptOption("base64", encryptPassword);
        Assertions.assertEquals("c2VhdHVubmVs", encryptUsername);
        Assertions.assertEquals("c2VhdHVubmVsX3Bhc3N3b3Jk", encryptPassword);
        Assertions.assertEquals(decryptUsername, username);
        Assertions.assertEquals(decryptPassword, password);
    }

    public static class Base64ConfigShade implements ConfigShade {

        private static final Base64.Encoder ENCODER = Base64.getEncoder();

        private static final Base64.Decoder DECODER = Base64.getDecoder();

        private static final String IDENTIFIER = "base64";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return ENCODER.encodeToString(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String decrypt(String content) {
            return new String(DECODER.decode(content));
        }
    }
}
