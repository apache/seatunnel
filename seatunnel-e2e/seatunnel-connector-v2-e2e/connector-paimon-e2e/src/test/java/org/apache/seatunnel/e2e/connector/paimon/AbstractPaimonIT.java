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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractPaimonIT extends TestSuiteBase {

    protected static final String NAMESPACE = "paimon";
    protected static final String TARGET_TABLE = "st_test";
    protected static final String FAKE_TABLE1 = "FakeTable1";
    protected static final String FAKE_DATABASE1 = "FakeDatabase1";
    protected static final String FAKE_TABLE2 = "FakeTable1";
    protected static final String FAKE_DATABASE2 = "FakeDatabase2";
    protected boolean isWindows;
    protected boolean changeLogEnabled = false;

    protected Table getTable(String dbName, String tbName) {
        try {
            return getCatalog().getTable(getIdentifier(dbName, tbName));
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }

    private Identifier getIdentifier(String dbName, String tbName) {
        return Identifier.create(dbName, tbName);
    }

    private Catalog getCatalog() {
        Options options = new Options();
        String warehouse =
                String.format(
                        "%s%s/%s",
                        isWindows ? "" : "file://",
                        AbstractTestContainer.HOST_VOLUME_MOUNT_PATH,
                        NAMESPACE);
        options.set("warehouse", warehouse);
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }
}
