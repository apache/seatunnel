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

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.core.starter.utils.CompressionUtils;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public abstract class AbstractPaimonIT extends TestSuiteBase {

    protected static String CATALOG_ROOT_DIR = "/tmp/";
    protected static final String NAMESPACE = "paimon";
    protected static final String NAMESPACE_TAR = "paimon.tar.gz";
    protected static final String CATALOG_DIR = CATALOG_ROOT_DIR + NAMESPACE + "/";
    protected static final String TARGET_TABLE = "st_test";
    protected static final String FAKE_TABLE1 = "FakeTable1";
    protected static final String FAKE_DATABASE1 = "FakeDatabase1";
    protected static final String FAKE_TABLE2 = "FakeTable1";
    protected static final String FAKE_DATABASE2 = "FakeDatabase2";
    private final String CATALOG_ROOT_DIR_WIN =
            "C:/Users/" + System.getProperty("user.name") + "/tmp/";
    private final String CATALOG_DIR_WIN = CATALOG_ROOT_DIR_WIN + NAMESPACE + "/";
    protected boolean isWindows;
    protected boolean changeLogEnabled = false;

    protected final ContainerExtendedFactory containerExtendedFactory =
            container -> {
                if (isWindows) {
                    FileUtils.deleteFile(CATALOG_ROOT_DIR_WIN + NAMESPACE_TAR);
                    FileUtils.deleteFile(CATALOG_ROOT_DIR_WIN + "paimon.tar");
                    FileUtils.createNewDir(CATALOG_ROOT_DIR_WIN);
                } else {
                    FileUtils.deleteFile(CATALOG_ROOT_DIR + NAMESPACE_TAR);
                    FileUtils.createNewDir(CATALOG_DIR);
                }

                container.execInContainer(
                        "sh",
                        "-c",
                        "cd "
                                + CATALOG_ROOT_DIR
                                + " && tar -czvf "
                                + NAMESPACE_TAR
                                + " "
                                + NAMESPACE);
                container.copyFileFromContainer(
                        CATALOG_ROOT_DIR + NAMESPACE_TAR,
                        (isWindows ? CATALOG_ROOT_DIR_WIN : CATALOG_ROOT_DIR) + NAMESPACE_TAR);
                if (isWindows) {
                    extractFilesWin();
                } else {
                    extractFiles();
                }
            };

    private void extractFiles() {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(
                "sh", "-c", "cd " + CATALOG_ROOT_DIR + " && tar -zxvf " + NAMESPACE_TAR);
        try {
            Process process = processBuilder.start();
            // wait command completed
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                log.info("Extract files successful.");
            } else {
                log.error("Extract files failed with exit code " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void extractFilesWin() {
        try {
            CompressionUtils.unGzip(
                    new File(CATALOG_ROOT_DIR_WIN + NAMESPACE_TAR), new File(CATALOG_ROOT_DIR_WIN));
            CompressionUtils.unTar(
                    new File(CATALOG_ROOT_DIR_WIN + "paimon.tar"), new File(CATALOG_ROOT_DIR_WIN));
        } catch (IOException | ArchiveException e) {
            throw new RuntimeException(e);
        }
    }

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
        if (isWindows) {
            options.set("warehouse", CATALOG_DIR_WIN);
        } else {
            options.set("warehouse", "file://" + CATALOG_DIR);
        }
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }
}
