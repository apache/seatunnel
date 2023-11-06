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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.api.table.catalog.CatalogOptions.TABLE_NAMES;
import static org.apache.seatunnel.common.constants.CollectionConstants.PLUGIN_NAME;

public class CatalogTableUtilTest {
    @Test
    public void testSimpleSchemaParse() throws FileNotFoundException, URISyntaxException {
        String path = getTestConfigFile("/conf/simple.schema.conf");
        Config config = ConfigFactory.parseFile(new File(path));
        SeaTunnelRowType seaTunnelRowType =
                CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();
        Assertions.assertNotNull(seaTunnelRowType);
        Assertions.assertEquals(seaTunnelRowType.getFieldType(1), ArrayType.BYTE_ARRAY_TYPE);
        Assertions.assertEquals(seaTunnelRowType.getFieldType(2), BasicType.STRING_TYPE);
        Assertions.assertEquals(seaTunnelRowType.getFieldType(10), new DecimalType(30, 8));
        Assertions.assertEquals(seaTunnelRowType.getFieldType(11), BasicType.VOID_TYPE);
        Assertions.assertEquals(seaTunnelRowType.getFieldType(12), PrimitiveByteArrayType.INSTANCE);
    }

    @Test
    public void testComplexSchemaParse() throws FileNotFoundException, URISyntaxException {
        String path = getTestConfigFile("/conf/complex.schema.conf");
        Config config = ConfigFactory.parseFile(new File(path));
        SeaTunnelRowType seaTunnelRowType =
                CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();
        Assertions.assertNotNull(seaTunnelRowType);
        Assertions.assertEquals(
                seaTunnelRowType.getFieldType(0),
                new MapType<>(
                        BasicType.STRING_TYPE,
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)));
        Assertions.assertEquals(
                seaTunnelRowType.getFieldType(1),
                new MapType<>(
                        BasicType.STRING_TYPE,
                        new MapType<>(BasicType.STRING_TYPE, ArrayType.INT_ARRAY_TYPE)));
        Assertions.assertEquals(seaTunnelRowType.getTotalFields(), 18);
        Assertions.assertEquals(seaTunnelRowType.getFieldType(17).getSqlType(), SqlType.ROW);
        SeaTunnelRowType nestedRowFieldType = (SeaTunnelRowType) seaTunnelRowType.getFieldType(17);
        Assertions.assertEquals(
                "map", nestedRowFieldType.getFieldName(nestedRowFieldType.indexOf("map")));
        Assertions.assertEquals(
                "row", nestedRowFieldType.getFieldName(nestedRowFieldType.indexOf("row")));
    }

    @Test
    public void testSpecialSchemaParse() throws FileNotFoundException, URISyntaxException {
        String path = getTestConfigFile("/conf/config_special_schema.conf");
        Config config = ConfigFactory.parseFile(new File(path));
        SeaTunnelRowType seaTunnelRowType =
                CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();
        Assertions.assertEquals(seaTunnelRowType.getTotalFields(), 12);
        Assertions.assertEquals(seaTunnelRowType.getFieldType(5).getSqlType(), SqlType.BYTES);
        Assertions.assertEquals(seaTunnelRowType.getFieldName(6), "t.date");
    }

    @Test
    public void testCatalogUtilGetCatalogTable() throws FileNotFoundException, URISyntaxException {
        String path = getTestConfigFile("/conf/getCatalogTable.conf");
        Config config = ConfigFactory.parseFile(new File(path));
        Config source = config.getConfigList("source").get(0);
        ReadonlyConfig sourceReadonlyConfig = ReadonlyConfig.fromConfig(source);
        List<CatalogTable> catalogTables =
                CatalogTableUtil.getCatalogTables(
                        sourceReadonlyConfig, Thread.currentThread().getContextClassLoader());
        Assertions.assertEquals(2, catalogTables.size());
        Assertions.assertEquals(
                TableIdentifier.of("InMemory", TablePath.of("st.public.table1")),
                catalogTables.get(0).getTableId());
        Assertions.assertEquals(
                TableIdentifier.of("InMemory", TablePath.of("st.public.table2")),
                catalogTables.get(1).getTableId());
        // test empty tables
        Config emptyTableSource =
                source.withValue(
                        TABLE_NAMES.key(), ConfigValueFactory.fromIterable(new ArrayList<>()));
        ReadonlyConfig emptyReadonlyConfig = ReadonlyConfig.fromConfig(emptyTableSource);
        Assertions.assertThrows(
                SeaTunnelException.class,
                () ->
                        CatalogTableUtil.getCatalogTables(
                                emptyReadonlyConfig,
                                Thread.currentThread().getContextClassLoader()));
        // test unknown catalog
        Config cannotFindCatalogSource =
                source.withValue(PLUGIN_NAME, ConfigValueFactory.fromAnyRef("unknownCatalog"));
        ReadonlyConfig cannotFindCatalogReadonlyConfig =
                ReadonlyConfig.fromConfig(cannotFindCatalogSource);
        Assertions.assertThrows(
                SeaTunnelException.class,
                () ->
                        CatalogTableUtil.getCatalogTables(
                                cannotFindCatalogReadonlyConfig,
                                Thread.currentThread().getContextClassLoader()));
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = CatalogTableUtilTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
