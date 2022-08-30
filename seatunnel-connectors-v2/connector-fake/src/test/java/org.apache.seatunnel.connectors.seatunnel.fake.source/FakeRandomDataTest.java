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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

public class FakeRandomDataTest {

    @ParameterizedTest
    @ValueSource(strings = {"complex.schema.conf", "simple.schema.conf"})
    public void testComplexSchemaParse(String conf) throws FileNotFoundException, URISyntaxException {
        Config testConfigFile = getTestConfigFile(conf);
        SeaTunnelSchema seatunnelSchema = SeaTunnelSchema.buildWithConfig(testConfigFile);
        FakeRandomData fakeRandomData = new FakeRandomData(seatunnelSchema);
        SeaTunnelRow seaTunnelRow = fakeRandomData.randomRow();
        Assertions.assertNotNull(seaTunnelRow);
        Object[] fields = seaTunnelRow.getFields();
        Assertions.assertNotNull(fields);
        SeaTunnelRowType seaTunnelRowType = seatunnelSchema.getSeaTunnelRowType();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getSqlType() != SqlType.NULL) {
                Assertions.assertNotNull(fields[i]);
            } else {
                Assertions.assertSame(fields[i], Void.TYPE);
            }
            if (fieldTypes[i].getSqlType() == SqlType.MAP) {
                Assertions.assertTrue(fields[i] instanceof Map);
                Map<?, ?> field = (Map) fields[i];
                field.forEach((k, v) -> Assertions.assertTrue(k != null && v != null));
            }
            if (fieldTypes[i].getSqlType() == SqlType.ARRAY) {
                Assertions.assertTrue(fields[i].getClass().isArray());
                Assertions.assertNotNull(Array.get(fields[i], 0));
            }
        }
    }

    private Config getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        if (!configFile.startsWith("/")) {
            configFile = "/" + configFile;
        }
        URL resource = FakeRandomDataTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        String path = Paths.get(resource.toURI()).toString();
        Config config = ConfigFactory.parseFile(new File(path));
        assert config.hasPath("schema");
        return config.getConfig("schema");
    }

}
