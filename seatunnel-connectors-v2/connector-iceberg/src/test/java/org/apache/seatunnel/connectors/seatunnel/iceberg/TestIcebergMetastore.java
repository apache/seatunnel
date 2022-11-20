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

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;

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

        HiveCatalog catalog = (HiveCatalog) new IcebergCatalogFactory("seatunnel",
            HIVE,
            "file://" + warehousePath,
            METASTORE_URI)
            .create();
        catalog.createNamespace(Namespace.of("test_database"));
        Assertions.assertTrue(catalog.namespaceExists(Namespace.of("test_database")));
    }

    @AfterEach
    public void close() throws Exception {
        METASTORE.stop();
    }
}
