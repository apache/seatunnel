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

package org.apache.seatunnel.connectors.seatunnel.iceberg;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;

public class TestIcebergMetastore {

    private static TestHiveMetastore METASTORE = null;
    private static String METASTORE_URI;

    @BeforeEach
    public void start() {
        METASTORE = new TestHiveMetastore();
        METASTORE.start();
        METASTORE_URI = METASTORE.hiveConf().get(HiveConf.ConfVars.METASTOREURIS.varname);
    }

    @Disabled("Disabled because system environment does not support to run this test")
    @Test
    public void testUseHiveMetastore() {
        String warehousePath = "/tmp/seatunnel/iceberg/hive/";
        new File(warehousePath).mkdirs();

        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HIVE.getType());
        catalogProps.put("warehouse", "file://" + warehousePath);
        catalogProps.put("uri", METASTORE_URI);

        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);

        HiveCatalog catalog =
                (HiveCatalog)
                        new IcebergCatalogLoader(
                                        new IcebergSinkConfig(ReadonlyConfig.fromMap(configs)))
                                .loadCatalog();
        catalog.createNamespace(Namespace.of("test_database"));
        Assertions.assertTrue(catalog.namespaceExists(Namespace.of("test_database")));
    }

    @AfterEach
    public void close() throws Exception {
        METASTORE.stop();
    }
}
