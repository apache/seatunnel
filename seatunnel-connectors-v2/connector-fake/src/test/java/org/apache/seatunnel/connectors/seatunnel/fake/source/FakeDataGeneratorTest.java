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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
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
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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

    @ParameterizedTest
    @ValueSource(strings = {"fake-data.column.conf"})
    public void testColumnDataParse(String conf) throws FileNotFoundException, URISyntaxException {
        ReadonlyConfig testConfig = getTestConfigFile(conf);
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(testConfig);
        FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(fakeConfig);
        List<SeaTunnelRow> seaTunnelRows =
                fakeDataGenerator.generateFakedRows(fakeConfig.getRowNum());
        seaTunnelRows.forEach(
                seaTunnelRow -> {
                    Assertions.assertEquals(
                            seaTunnelRow.getField(0).toString(), "Andersen's Fairy Tales");
                    Assertions.assertEquals(seaTunnelRow.getField(1).toString().length(), 100);
                    Assertions.assertEquals(seaTunnelRow.getField(2).toString(), "10.1");
                    Assertions.assertNotNull(seaTunnelRow.getField(3).toString());
                    Assertions.assertNotNull(seaTunnelRow.getField(4).toString());
                    //  VectorType.VECTOR_FLOAT_TYPE
                    Assertions.assertEquals(
                            8, ((ByteBuffer) seaTunnelRow.getField(5)).capacity() / 4);
                    // VectorType.VECTOR_BINARY_TYPE
                    Assertions.assertEquals(
                            16, ((ByteBuffer) seaTunnelRow.getField(6)).capacity() * 8);
                    // VectorType.VECTOR_FLOAT16_TYPE
                    Assertions.assertEquals(
                            8, ((ByteBuffer) seaTunnelRow.getField(7)).capacity() / 2);
                    // VectorType.VECTOR_BFLOAT16_TYPE
                    Assertions.assertEquals(
                            8, ((ByteBuffer) seaTunnelRow.getField(8)).capacity() / 2);
                    // VectorType.VECTOR_SPARSE_FLOAT_TYPE
                    Assertions.assertEquals(8, ((Map) seaTunnelRow.getField(9)).size());
                    Assertions.assertNotNull(seaTunnelRow.getField(10).toString());
                    Assertions.assertNotNull(seaTunnelRow.getField(11).toString());
                    Assertions.assertEquals(
                            436,
                            seaTunnelRow.getBytesSize(
                                    new SeaTunnelRowType(
                                            new String[] {
                                                "field1", "field2", "field3", "field4", "field5",
                                                "field6", "field7", "field8", "field9", "field10",
                                                "field11", "field12", "field13", "field14",
                                                "field15", "field16"
                                            },
                                            new SeaTunnelDataType<?>[] {
                                                BasicType.STRING_TYPE,
                                                BasicType.STRING_TYPE,
                                                BasicType.FLOAT_TYPE,
                                                BasicType.FLOAT_TYPE,
                                                BasicType.DOUBLE_TYPE,
                                                VectorType.VECTOR_FLOAT_TYPE,
                                                VectorType.VECTOR_BINARY_TYPE,
                                                VectorType.VECTOR_FLOAT16_TYPE,
                                                VectorType.VECTOR_BFLOAT16_TYPE,
                                                VectorType.VECTOR_SPARSE_FLOAT_TYPE,
                                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                                LocalTimeType.LOCAL_TIME_TYPE,
                                                LocalTimeType.LOCAL_TIME_TYPE,
                                                LocalTimeType.LOCAL_DATE_TYPE,
                                                LocalTimeType.LOCAL_DATE_TYPE
                                            })));
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"fake-data.schema.default.conf"})
    public void testDataParse(String conf) throws FileNotFoundException, URISyntaxException {
        ReadonlyConfig testConfig = getTestConfigFile(conf);
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(testConfig);
        FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(fakeConfig);
        List<SeaTunnelRow> seaTunnelRows =
                fakeDataGenerator.generateFakedRows(fakeConfig.getRowNum());
        seaTunnelRows.forEach(
                seaTunnelRow -> {
                    Assertions.assertInstanceOf(Long.class, seaTunnelRow.getField(0));
                    Assertions.assertInstanceOf(String.class, seaTunnelRow.getField(1));
                    Assertions.assertInstanceOf(Integer.class, seaTunnelRow.getField(2));
                    Assertions.assertInstanceOf(LocalDateTime.class, seaTunnelRow.getField(3));
                    Assertions.assertInstanceOf(LocalTime.class, seaTunnelRow.getField(4));
                    Assertions.assertInstanceOf(LocalDate.class, seaTunnelRow.getField(5));
                });
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
