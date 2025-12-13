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
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonBaseOptions;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.FileBasedPrivilegeManagerLoader;
import org.apache.paimon.privilege.PrivilegeType;
import org.apache.paimon.privilege.PrivilegedCatalog;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_13, TestContainerId.SPARK_2_4},
        disabledReason =
                "Paimon does not support flink 1.13, Spark 2.4.6 has a jar package(zstd-jni-version.jar) version compatibility issue.")
public class PaimonIT extends TestSuiteBase implements TestResource {
    private final String rootUser = "root";
    private final String rootPassword = "123456";
    private final String paimonUser = "paimon";
    private final String paimonUserPassword = "123456";

    private PrivilegedCatalog privilegedCatalog;
    private final String DATABASE_NAME = "default";
    private final String TABLE_NAME = "st_test_p";

    private static final String NAMESPACE = "paimon";
    protected static String hostName = System.getProperty("user.name");
    protected static final String CONTAINER_VOLUME_MOUNT_PATH = "/tmp/seatunnel_mnt";

    protected static final boolean isWindows =
            System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS");
    public static final String HOST_VOLUME_MOUNT_PATH =
            isWindows
                    ? String.format("C:/Users/%s/tmp/seatunnel_mnt", hostName)
                    : CONTAINER_VOLUME_MOUNT_PATH;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Path schemaPath = ContainerUtil.getResourcesFile("/schema-0.json").toPath();
                container.copyFileToContainer(
                        MountableFile.forHostPath(schemaPath),
                        "/tmp/seatunnel_mnt/paimon/default.db/st_test/schema/schema-0");
                container.copyFileToContainer(
                        MountableFile.forHostPath(schemaPath),
                        "/tmp/seatunnel_mnt/paimon/default.db/st_test_p/schema/schema-0");
                container.copyFileToContainer(
                        MountableFile.forHostPath(schemaPath),
                        "/tmp/seatunnel_mnt/paimon/default.db/st_test_p1/schema/schema-0");
                container.execInContainer("chmod", "777", "-R", "/tmp/seatunnel_mnt/");
            };

    @TestTemplate
    public void testWriteAndReadPaimon(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_paimon.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult readResult = container.executeJob("/paimon_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
        Container.ExecResult readProjectionResult =
                container.executeJob("/paimon_projection_to_assert.conf");
        Assertions.assertEquals(0, readProjectionResult.getExitCode());
    }

    @TestTemplate
    public void testMultiTableRead(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_paimon.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult textWriteResult2 = container.executeJob("/fake_to_paimon_2.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult multiReadResult =
                container.executeJob("/paimon-to-assert-with-multipletable.conf");
        Assertions.assertEquals(0, multiReadResult.getExitCode());
    }

    @Override
    public void startUp() throws Exception {}

    @Override
    @AfterEach
    public void tearDown() throws Exception {}

    private void initPrivilege(List<PrivilegeType> privilegeTypes, String warehouse) {
        org.apache.paimon.options.Options catalogOptions = new org.apache.paimon.options.Options();
        catalogOptions.set(PaimonBaseOptions.WAREHOUSE.key(), warehouse);
        final CatalogContext catalogContext = CatalogContext.create(catalogOptions);

        FileIO fileIO = new ResolvingFileIO();
        fileIO.configure(catalogContext);

        privilegedCatalog =
                new PrivilegedCatalog(
                        CatalogFactory.createCatalog(catalogContext),
                        new FileBasedPrivilegeManagerLoader(
                                warehouse, fileIO, rootUser, rootPassword));
        if (!privilegedCatalog.privilegeManager().privilegeEnabled()) {
            privilegedCatalog.privilegeManager().initializePrivilege(rootPassword);
        }

        // create user and grant privilege on table
        privilegedCatalog.privilegeManager().createUser(paimonUser, paimonUserPassword);
        String fullTableName = Identifier.create(DATABASE_NAME, TABLE_NAME).getFullName();
        String fullTableName1 = Identifier.create(DATABASE_NAME, "st_test_p1").getFullName();
        privilegedCatalog.privilegeManager().grant(paimonUser, "", PrivilegeType.CREATE_DATABASE);
        privilegedCatalog
                .privilegeManager()
                .grant(paimonUser, DATABASE_NAME, PrivilegeType.DROP_DATABASE);
        privilegedCatalog
                .privilegeManager()
                .grant(paimonUser, fullTableName, PrivilegeType.DROP_TABLE);
        privilegedCatalog
                .privilegeManager()
                .grant(paimonUser, fullTableName1, PrivilegeType.DROP_TABLE);
        privilegedCatalog
                .privilegeManager()
                .grant(paimonUser, DATABASE_NAME, PrivilegeType.CREATE_TABLE);
        if (!CollectionUtils.isEmpty(privilegeTypes)) {
            for (PrivilegeType type : privilegeTypes) {
                privilegedCatalog.privilegeManager().grant(paimonUser, fullTableName, type);
                privilegedCatalog.privilegeManager().grant(paimonUser, fullTableName1, type);
            }
        }
    }

    /** User not grant read privilege read data test cases for the Paimon table */
    @TestTemplate
    public void privilegeEnabledPaimonSourceAuthorized(TestContainer container) throws Exception {
        String warehouse = "/tmp/seatunnel_mnt/paimon";
        List<PrivilegeType> privilegeTypes = new ArrayList<>();
        privilegeTypes.add(PrivilegeType.SELECT);
        privilegeTypes.add(PrivilegeType.INSERT);
        initPrivilege(privilegeTypes, warehouse);
        // fake to paimon
        Container.ExecResult execResult = container.executeJob("/fake_to_paimon_privilege.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // paimon to paimon
        Container.ExecResult execResult1 = container.executeJob("/paimon_to_paimon_privilege.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
    }

    /** User not grant read privilege read data test cases for the Paimon table */
    @TestTemplate
    public void privilegeEnabledPaimonSourceUnAuthorized(TestContainer container) throws Exception {
        String warehouse = "/tmp/seatunnel_mnt/paimon";
        List<PrivilegeType> privilegeTypes = new ArrayList<>();
        privilegeTypes.add(PrivilegeType.INSERT);
        initPrivilege(privilegeTypes, warehouse);
        // fake to paimon
        Container.ExecResult execResult = container.executeJob("/fake_to_paimon_privilege1.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // paimon to paimon
        Container.ExecResult execResult1 =
                container.executeJob("/paimon_to_paimon_privilege1.conf");
        Assertions.assertEquals(1, execResult1.getExitCode());
    }

    @TestTemplate
    public void jobFinishedCleanTmpFiles(TestContainer container) throws Exception {
        // fake to paimon
        Container.ExecResult execResult =
                container.executeJob("/fake_to_paimon_with_change_log_tmp.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // check job finished clean up tmp files
        String hostName = System.getProperty("user.name");
        boolean isWindows =
                System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS");
        String tmpDir =
                isWindows
                        ? String.format("C:/Users/%s/tmp/seatunnel_mnt/paimon_tmp", hostName)
                        : "/tmp/seatunnel_mnt/paimon_tmp";
        List<File> files = FileUtils.listFile(tmpDir);
        Assertions.assertTrue(CollectionUtils.isEmpty(files));
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Spark and Flink engine can not auto create paimon table on worker node in local file(e.g flink tm) by savemode feature which can lead error")
    @TestTemplate
    public void testSinkBranch(TestContainer container) throws Exception {

        String testBranchName = "test_branch";
        FileStoreTable table = (FileStoreTable) getTable(DATABASE_NAME, TABLE_NAME);
        List<String> branches = table.branchManager().branches();
        if (!branches.contains(testBranchName)) {
            table.createBranch(testBranchName);
        }
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_paimon_branch.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        long rowCount = getTableRowCount(table);
        Assertions.assertEquals(0, rowCount);

        FileStoreTable fileStoreTableWithBranch = table.switchToBranch(testBranchName);
        rowCount = getTableRowCount(fileStoreTableWithBranch);
        Assertions.assertEquals(10001, rowCount);
    }

    private Table getTable(String dbName, String tbName) {
        Options options = new Options();
        String warehouse =
                String.format(
                        "%s%s/%s", isWindows ? "" : "file://", HOST_VOLUME_MOUNT_PATH, NAMESPACE);
        options.set("warehouse", warehouse);
        try {
            Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            return catalog.getTable(Identifier.create(dbName, tbName));
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException("table not exist");
        }
    }

    private long getTableRowCount(FileStoreTable table) {
        try {
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan.Plan plan = readBuilder.newScan().plan();
            TableRead tableRead = readBuilder.newRead();

            long count = 0;
            try (RecordReader<InternalRow> reader = tableRead.createReader(plan);
                    RecordReaderIterator<InternalRow> iterator =
                            new RecordReaderIterator<>(reader)) {
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                }
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read data count from table", e);
        }
    }
}
