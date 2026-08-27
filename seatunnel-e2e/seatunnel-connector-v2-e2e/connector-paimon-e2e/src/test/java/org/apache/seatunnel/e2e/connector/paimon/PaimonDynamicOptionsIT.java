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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_13, TestContainerId.SPARK_2_4},
        disabledReason =
                "Paimon does not support flink 1.13, Spark 2.4.6 has a jar package(zstd-jni-version.jar) version compatibility issue.")
public class PaimonDynamicOptionsIT extends TestSuiteBase implements TestResource {

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

    @Override
    public void startUp() throws Exception {}

    @Override
    @AfterEach
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testPaimonDynamicOptionsOfBranch(TestContainer container) throws Exception {
        String testBranchName = "test-branch";
        FileStoreTable table = (FileStoreTable) getTable(DATABASE_NAME, TABLE_NAME);
        List<String> branches = table.branchManager().branches();
        if (!branches.contains(testBranchName)) {
            table.createBranch(testBranchName);
        }
        FileStoreTable fileStoreTableWithBranch = table.switchToBranch(testBranchName);
        TableWriteImpl<?> write = fileStoreTableWithBranch.newWrite("3494269");

        write.write(createTestRow(1L, "First record"));
        write.write(createTestRow(2L, "Second record"));
        write.write(createTestRow(3L, "Third record"));
        write.write(createTestRow(4L, "Fourth record"));
        write.write(createTestRow(5L, "Fifth record"));

        List<CommitMessage> commitMessages = write.prepareCommit(false, 1);
        try (TableCommitImpl commit = fileStoreTableWithBranch.newCommit("3494269")) {
            commit.commit(commitMessages);
        }
        write.close();

        Container.ExecResult textWriteResult =
                container.executeJob("/paimon_to_assert_with_dynamic_options_of_branch.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
    }

    @TestTemplate
    public void testPaimonDynamicOptionsOfTag(TestContainer container) throws Exception {
        String testTag1 = "test-tag1";
        String testTag2 = "test-tag2";
        FileStoreTable table = (FileStoreTable) getTable(DATABASE_NAME, TABLE_NAME);

        TableWriteImpl<?> write = table.newWrite("3494269");

        write.write(createTestRow(1L, "First record"));
        write.write(createTestRow(2L, "Second record"));
        write.write(createTestRow(3L, "Third record"));
        write.write(createTestRow(4L, "Fourth record"));
        write.write(createTestRow(5L, "Fifth record"));

        List<CommitMessage> commitMessages = write.prepareCommit(false, 1);
        try (TableCommitImpl commit = table.newCommit("3494269")) {
            commit.commit(commitMessages);
        }
        table.createTag(testTag1);

        Container.ExecResult textWriteTag1 =
                container.executeJob("/paimon_to_assert_with_dynamic_options_of_tag1.conf");
        Assertions.assertEquals(0, textWriteTag1.getExitCode());

        write.write(createTestRow(6L, "Sixth record"));
        write.write(createTestRow(7L, "Seventh record"));
        commitMessages = write.prepareCommit(false, 1);
        try (TableCommitImpl commit = table.newCommit("3494269")) {
            commit.commit(commitMessages);
        }
        table.createTag(testTag2);
        write.close();

        Container.ExecResult textWriteTag2 =
                container.executeJob("/paimon_to_assert_with_dynamic_options_of_tag2.conf");
        Assertions.assertEquals(0, textWriteTag2.getExitCode());

        Container.ExecResult textWriteResult =
                container.executeJob("/paimon_to_assert_with_dynamic_options_of_incr_tag.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
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

    private GenericRow createTestRow(Long pkId, String description) {
        Map<String, String> mapData = new HashMap<>();
        mapData.put("key1", "value1_" + pkId);
        mapData.put("key2", "value2_" + pkId);
        mapData.put("description", description);
        BinaryArray keyArray = new BinaryArray();
        BinaryArrayWriter keyWriter =
                new BinaryArrayWriter(
                        keyArray, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));
        keyWriter.writeString(0, BinaryString.fromString("key1"));
        keyWriter.writeString(1, BinaryString.fromString("key2"));
        keyWriter.writeString(2, BinaryString.fromString("description"));
        keyWriter.complete();

        BinaryArray valueArray = new BinaryArray();
        BinaryArrayWriter valueWriter =
                new BinaryArrayWriter(
                        valueArray, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));
        valueWriter.writeString(0, BinaryString.fromString("value1_" + pkId));
        valueWriter.writeString(1, BinaryString.fromString("value2_" + pkId));
        valueWriter.writeString(2, BinaryString.fromString(description));
        valueWriter.complete();

        BinaryMap binaryMap = BinaryMap.valueOf(keyArray, valueArray);
        BinaryArray intArray = new BinaryArray();
        BinaryArrayWriter intArrayWriter =
                new BinaryArrayWriter(
                        intArray, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.INT()));
        intArrayWriter.writeInt(0, pkId.intValue());
        intArrayWriter.writeInt(1, pkId.intValue() * 10);
        intArrayWriter.writeInt(2, pkId.intValue() * 100);
        intArrayWriter.complete();
        return GenericRow.of(
                pkId,
                binaryMap,
                intArray,
                BinaryString.fromString(description + "_" + pkId),
                pkId % 2 == 0,
                (byte) (pkId % 128),
                (short) (pkId * 10),
                pkId.intValue() * 100,
                pkId * 1000L,
                pkId.floatValue() + 0.5f,
                pkId.doubleValue() + 0.123,
                Decimal.fromBigDecimal(new BigDecimal(pkId + ".12345678"), 30, 8),
                BinaryString.fromString("bytes_" + pkId).toBytes(),
                DateTimeUtils.toInternal(LocalDate.of(2024, 1, pkId.intValue() % 28 + 1)),
                Timestamp.fromLocalDateTime(
                        LocalDateTime.of(
                                2024,
                                1,
                                pkId.intValue() % 28 + 1,
                                pkId.intValue() % 24,
                                pkId.intValue() % 60,
                                0)),
                DateTimeUtils.toInternal(
                        LocalTime.of(pkId.intValue() % 24, pkId.intValue() % 60, 0)));
    }
}
