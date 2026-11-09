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

package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.constants.MetaLakeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class MetalakeLocaleTest {

    private static final String JOB_CONFIG =
            "env {\n"
                    + "  metalake_enabled = true\n"
                    + "  metalake_type = \"gravitino\"\n"
                    + "  metalake_url = \"http://localhost:8090/api/metalakes/test/\"\n"
                    + "}\n"
                    + "source = []\n"
                    + "sink = []\n"
                    + "transform = []\n";

    @Test
    public void testCreateTypeMapperIsIndependentOfDefaultLocale() {
        Locale defaultLocale = Locale.getDefault();
        try {
            // The registry is keyed by the lower-cased type name while the lookup
            // lower-cases the enum constant name, so a locale dependent conversion
            // would not find the registered entry.
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            Assertions.assertNotNull(MetaLakeFactory.createTypeMapper(MetaLakeType.GRAVITINO));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    @Test
    public void testGetMetalakeConfigIsIndependentOfDefaultLocale() {
        Locale defaultLocale = Locale.getDefault();
        try {
            // The configured type name "gravitino" upper-cases to "GRAVİTİNO" in a
            // Turkish locale, which is not a MetaLakeType constant.
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            Config config = ConfigFactory.parseString(JOB_CONFIG);
            Config metalakeConfig = MetalakeConfigUtils.getMetalakeConfig(config);
            Assertions.assertNotNull(metalakeConfig);
            // The env block must survive the conversion untouched.
            Config envConfig = metalakeConfig.getConfig(Constants.ENV);
            Assertions.assertTrue(envConfig.getBoolean(EnvCommonOptions.METALAKE_ENABLED.key()));
            Assertions.assertEquals(
                    "gravitino", envConfig.getString(EnvCommonOptions.METALAKE_TYPE.key()));
            Assertions.assertEquals(
                    "http://localhost:8090/api/metalakes/test/",
                    envConfig.getString(EnvCommonOptions.METALAKE_URL.key()));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }
}
