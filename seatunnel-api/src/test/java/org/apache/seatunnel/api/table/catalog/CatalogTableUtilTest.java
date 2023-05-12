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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class CatalogTableUtilTest {
    @Test
    public void testSimpleSchemaParse() throws FileNotFoundException, URISyntaxException {
        String path = getTestConfigFile("/conf/simple.schema.conf");
        Config config =
                ConfigFactory.parseFile(new File(path))
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
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
        Config config =
                ConfigFactory.parseFile(new File(path))
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
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
        Assertions.assertEquals(seaTunnelRowType.getFieldType(17).getSqlType(), SqlType.ROW);
        SeaTunnelRowType nestedRowFieldType = (SeaTunnelRowType) seaTunnelRowType.getFieldType(17);
        Assertions.assertEquals(
                "map", nestedRowFieldType.getFieldName(nestedRowFieldType.indexOf("map")));
        Assertions.assertEquals(
                "row", nestedRowFieldType.getFieldName(nestedRowFieldType.indexOf("row")));
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
