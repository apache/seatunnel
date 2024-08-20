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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FakeDataGeneratorTest {

    @ParameterizedTest
    @ValueSource(strings = {"complex.schema.conf", "simple.schema.conf"})
    public void testComplexSchemaParse(String conf)
            throws FileNotFoundException, URISyntaxException {
        ReadonlyConfig testConfig = getTestConfigFile(conf);
        SeaTunnelRowType seaTunnelRowType =
                CatalogTableUtil.buildWithConfig(testConfig).getSeaTunnelRowType();
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(testConfig);
        FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(fakeConfig);
        List<SeaTunnelRow> seaTunnelRows =
                fakeDataGenerator.generateFakedRows(fakeConfig.getRowNum());
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

    @ParameterizedTest
    @ValueSource(strings = {"fake-data.schema.conf"})
    public void testRowDataParse(String conf) throws FileNotFoundException, URISyntaxException {
        SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1.setRowKind(RowKind.INSERT);
        row1.setTableId(TablePath.DEFAULT.getFullName());
        SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2.setRowKind(RowKind.INSERT);
        row2.setTableId(TablePath.DEFAULT.getFullName());
        SeaTunnelRow row3 = new SeaTunnelRow(new Object[] {3L, "C", 100});
        row3.setRowKind(RowKind.INSERT);
        row3.setTableId(TablePath.DEFAULT.getFullName());
        SeaTunnelRow row1UpdateBefore = new SeaTunnelRow(new Object[] {1L, "A", 100});
        row1UpdateBefore.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        SeaTunnelRow row1UpdateAfter = new SeaTunnelRow(new Object[] {1L, "A_1", 100});
        row1UpdateAfter.setTableId(TablePath.DEFAULT.getFullName());
        row1UpdateAfter.setRowKind(RowKind.UPDATE_AFTER);
        SeaTunnelRow row2Delete = new SeaTunnelRow(new Object[] {2L, "B", 100});
        row2Delete.setTableId(TablePath.DEFAULT.getFullName());
        row2Delete.setRowKind(RowKind.DELETE);
        List<SeaTunnelRow> expected =
                Arrays.asList(row1, row2, row3, row1UpdateBefore, row1UpdateAfter, row2Delete);

        ReadonlyConfig testConfig = getTestConfigFile(conf);
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(testConfig);
        FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(fakeConfig);
        List<SeaTunnelRow> seaTunnelRows =
                fakeDataGenerator.generateFakedRows(fakeConfig.getRowNum());
        Assertions.assertIterableEquals(expected, seaTunnelRows);
    }

    @ParameterizedTest
    @ValueSource(strings = {"fake-vector.conf"})
    public void testVectorParse(String conf) throws FileNotFoundException, URISyntaxException {
        ReadonlyConfig testConfig = getTestConfigFile(conf);
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(testConfig);
        FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(fakeConfig);
        List<SeaTunnelRow> seaTunnelRows =
                fakeDataGenerator.generateFakedRows(fakeConfig.getRowNum());
        seaTunnelRows.forEach(
                seaTunnelRow ->
                        Assertions.assertEquals(
                                65,
                                seaTunnelRow.getBytesSize(
                                        new SeaTunnelRowType(
                                                new String[] {
                                                    "field1", "field2", "field3", "field4", "field5"
                                                },
                                                new SeaTunnelDataType<?>[] {
                                                    VectorType.VECTOR_FLOAT_TYPE,
                                                    VectorType.VECTOR_BINARY_TYPE,
                                                    VectorType.VECTOR_FLOAT16_TYPE,
                                                    VectorType.VECTOR_BFLOAT16_TYPE,
                                                    VectorType.VECTOR_SPARSE_FLOAT_TYPE
                                                }))));
        Assertions.assertNotNull(seaTunnelRows);
    }

    private ReadonlyConfig getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
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
        return ReadonlyConfig.fromConfig(config.getConfig("FakeSource"));
    }
}
