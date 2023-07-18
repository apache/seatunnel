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

package org.apache.seatunnel.e2e.connector.iceberg.hadoop3;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;

@Slf4j
public class IcebergSinkIT extends TestSuiteBase implements TestResource {

    private static final TableIdentifier TABLE =
            TableIdentifier.of(Namespace.of("database1"), "sink");
    private static final String CATALOG_NAME = "seatunnel";
    private static final IcebergCatalogType CATALOG_TYPE = HADOOP;
    private static final String CATALOG_DIR = "/tmp/seatunnel/iceberg/hadoop3/";
    private static final String WAREHOUSE = "file://" + CATALOG_DIR;
    private static Catalog CATALOG;

    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get()));

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                container.copyFileToContainer(MountableFile.forHostPath(CATALOG_DIR), CATALOG_DIR);
            };

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        initializeIcebergTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
    }

    @TestTemplate
    public void testIcebergSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/iceberg/iceberg_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private void initializeIcebergTable() {
        CATALOG = new IcebergCatalogFactory(CATALOG_NAME, CATALOG_TYPE, WAREHOUSE, null).create();
        if (!CATALOG.tableExists(TABLE)) {
            CATALOG.createTable(TABLE, SCHEMA);
        }
    }

}
