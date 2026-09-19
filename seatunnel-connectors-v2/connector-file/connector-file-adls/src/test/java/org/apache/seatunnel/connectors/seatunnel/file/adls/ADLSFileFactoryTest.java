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

package org.apache.seatunnel.connectors.seatunnel.file.adls;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.adls.catalog.ADLSFileCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.file.adls.config.ADLSFileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.adls.sink.ADLSFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.adls.source.ADLSFileSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ADLSFileFactoryTest {

    @Test
    void discoversAdlsFactoriesByTheirPublicIdentifiers() {
        ClassLoader classLoader = ADLSFileFactoryTest.class.getClassLoader();

        TableSourceFactory source =
                FactoryUtil.discoverFactory(classLoader, TableSourceFactory.class, "ADLSFile");
        TableSinkFactory sink =
                FactoryUtil.discoverFactory(classLoader, TableSinkFactory.class, "ADLSFile");
        CatalogFactory catalog =
                FactoryUtil.discoverFactory(classLoader, CatalogFactory.class, "ADLS");

        Assertions.assertEquals(ADLSFileSourceFactory.class, source.getClass());
        Assertions.assertEquals("ADLSFile", source.factoryIdentifier());
        Assertions.assertEquals(ADLSFileSinkFactory.class, sink.getClass());
        Assertions.assertEquals("ADLSFile", sink.factoryIdentifier());
        Assertions.assertEquals(ADLSFileCatalogFactory.class, catalog.getClass());
        Assertions.assertEquals("ADLS", catalog.factoryIdentifier());
    }

    @Test
    void sourceSharedKeyRuleRequiresAccountKey() {
        TableSourceFactory source =
                FactoryUtil.discoverFactory(
                        ADLSFileFactoryTest.class.getClassLoader(),
                        TableSourceFactory.class,
                        "ADLSFile");
        Map<String, Object> config = sharedKeyConfig();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), "/input");
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.ORC);

        assertRequiresAccountKey(config, source.optionRule());
    }

    @Test
    void sinkSharedKeyRuleRequiresAccountKey() {
        TableSinkFactory sink =
                FactoryUtil.discoverFactory(
                        ADLSFileFactoryTest.class.getClassLoader(),
                        TableSinkFactory.class,
                        "ADLSFile");
        Map<String, Object> config = sharedKeyConfig();
        config.put(FileBaseSinkOptions.FILE_PATH.key(), "/output");

        assertRequiresAccountKey(config, sink.optionRule());
    }

    private static Map<String, Object> sharedKeyConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ADLSFileBaseOptions.ACCOUNT_NAME.key(), "testaccount");
        config.put(ADLSFileBaseOptions.CONTAINER.key(), "files");
        config.put(ADLSFileBaseOptions.AUTH_TYPE.key(), ADLSFileBaseOptions.AuthType.SHARED_KEY);
        config.put(ADLSFileBaseOptions.ACCOUNT_KEY.key(), "sentinel-key");
        return config;
    }

    private static void assertRequiresAccountKey(Map<String, Object> config, OptionRule rule) {
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.remove(ADLSFileBaseOptions.ACCOUNT_KEY.key());
        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(config, rule));
        Assertions.assertTrue(error.getMessage().contains(ADLSFileBaseOptions.ACCOUNT_KEY.key()));
    }

    private static void validate(Map<String, Object> config, OptionRule rule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }
}
