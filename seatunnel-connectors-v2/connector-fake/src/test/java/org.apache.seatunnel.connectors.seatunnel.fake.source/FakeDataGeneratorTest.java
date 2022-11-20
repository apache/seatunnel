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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class FakeDataGeneratorTest {

    @ParameterizedTest
    @ValueSource(strings = {"complex.schema.conf", "simple.schema.conf"})
    public void testComplexSchemaParse(String conf) throws FileNotFoundException, URISyntaxException {
        Config testConfig = getTestConfigFile(conf);
        SeaTunnelSchema seaTunnelSchema = SeaTunnelSchema.buildWithConfig(testConfig.getConfig(SeaTunnelSchema.SCHEMA.key()));
        SeaTunnelRowType seaTunnelRowType = seaTunnelSchema.getSeaTunnelRowType();
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(testConfig);
        FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(seaTunnelSchema, fakeConfig);
        List<SeaTunnelRow> seaTunnelRows = fakeDataGenerator.generateFakedRows(fakeConfig.getRowNum());
        Assertions.assertNotNull(seaTunnelRows);
        Assertions.assertEquals(seaTunnelRows.size(), 10);
        for (SeaTunnelRow seaTunnelRow : seaTunnelRows) {
            for (int i = 0; i < seaTunnelRowType.getFieldTypes().length; i++) {
                switch (seaTunnelRowType.getFieldType(i).getSqlType()) {
                    case STRING:
                        Assertions.assertEquals(((String) seaTunnelRow.getField(i)).length(), 10);
                        break;
                    case BYTES:
                        Assertions.assertEquals(((byte[]) seaTunnelRow.getField(i)).length, 10);
                        break;
                    case ARRAY:
                        Assertions.assertEquals(((Object[]) seaTunnelRow.getField(i)).length, 10);
                        break;
                    case MAP:
                        Assertions.assertEquals(((Map<?, ?>) seaTunnelRow.getField(i)).size(), 10);
                        break;
                    default:
                        // do nothing
                        break;
                }
            }
        }
    }

    private Config getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        if (!configFile.startsWith("/")) {
            configFile = "/" + configFile;
        }
        URL resource = FakeDataGeneratorTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        String path = Paths.get(resource.toURI()).toString();
        Config config = ConfigFactory.parseFile(new File(path));
        assert config.hasPath("FakeSource");
        return config.getConfig("FakeSource");
    }

}
