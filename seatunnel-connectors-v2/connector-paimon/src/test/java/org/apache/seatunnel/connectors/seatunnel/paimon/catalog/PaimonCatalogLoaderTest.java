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

package org.apache.seatunnel.connectors.seatunnel.paimon.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;

import org.apache.paimon.options.CatalogOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PaimonCatalogLoaderTest {

    @Test
    public void shouldIncludeOssHadoopConfForOssWarehouse() {
        Map<String, String> hadoopConf = new HashMap<>();
        hadoopConf.put("fs.oss.accessKeyId", "access-key");
        hadoopConf.put("fs.oss.accessKeySecret", "access-secret");
        hadoopConf.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");

        PaimonCatalogLoader catalogLoader =
                new PaimonCatalogLoader(
                        new PaimonConfig(
                                ReadonlyConfig.fromMap(
                                        createProperties("oss://bucket/warehouse", hadoopConf))));

        Map<String, String> catalogOptions = catalogLoader.buildCatalogOptions();

        Assertions.assertEquals(
                "oss://bucket/warehouse", catalogOptions.get(CatalogOptions.WAREHOUSE.key()));
        Assertions.assertEquals("access-key", catalogOptions.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("access-secret", catalogOptions.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals(
                "oss-cn-hangzhou.aliyuncs.com", catalogOptions.get("fs.oss.endpoint"));
    }

    @Test
    public void shouldNotIncludeObjectStoreConfForLocalWarehouse() {
        Map<String, String> hadoopConf = new HashMap<>();
        hadoopConf.put("fs.oss.accessKeyId", "access-key");

        PaimonCatalogLoader catalogLoader =
                new PaimonCatalogLoader(
                        new PaimonConfig(
                                ReadonlyConfig.fromMap(
                                        createProperties("file:///tmp/paimon", hadoopConf))));

        Map<String, String> catalogOptions = catalogLoader.buildCatalogOptions();

        Assertions.assertFalse(catalogOptions.containsKey("fs.oss.accessKeyId"));
    }

    private Map<String, Object> createProperties(String warehouse, Map<String, String> hadoopConf) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(PaimonBaseOptions.CATALOG_NAME.key(), "paimon");
        properties.put(PaimonBaseOptions.WAREHOUSE.key(), warehouse);
        properties.put(PaimonBaseOptions.HADOOP_CONF.key(), hadoopConf);
        return properties;
    }
}
