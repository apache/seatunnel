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

package org.apache.seatunnel.e2e.connector.hbase;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.hbase.catalog.HbaseCatalog;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.groovy.util.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SEATUNNEL},
        disabledReason = "The hbase container authentication configuration is incorrect.")
public class HbaseIT extends TestSuiteBase implements TestResource {

    private static final String TABLE_NAME = "seatunnel_test";

    private static final String ASSIGN_CF_TABLE_NAME = "assign_cf_table";

    private static final String MULTI_TABLE_ONE_NAME = "hbase_sink_1";

    private static final String MULTI_TABLE_TWO_NAME = "hbase_sink_2";

    private static final String FAMILY_NAME = "info";

    private Connection hbaseConnection;

    private Admin admin;

    private TableName table;
    private TableName tableAssign;

    private HbaseCluster hbaseCluster;

    private Catalog catalog;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hbaseCluster = new HbaseCluster();
        hbaseConnection = hbaseCluster.startService();
        admin = hbaseConnection.getAdmin();
        // Create table for hbase sink test
        log.info("initial");
        hbaseCluster.createTable(TABLE_NAME, Arrays.asList(FAMILY_NAME));
        // Create table for hbase assign cf table sink test
        hbaseCluster.createTable(ASSIGN_CF_TABLE_NAME, Arrays.asList("cf1", "cf2"));
        table = TableName.valueOf(TABLE_NAME);
        tableAssign = TableName.valueOf(ASSIGN_CF_TABLE_NAME);

        // Create table for hbase multi-table sink test
        hbaseCluster.createTable(MULTI_TABLE_ONE_NAME, Arrays.asList(FAMILY_NAME));
        hbaseCluster.createTable(MULTI_TABLE_TWO_NAME, Arrays.asList(FAMILY_NAME));

        Map<String, Object> config = new HashMap<>();
        config.put(HbaseConfig.ZOOKEEPER_QUORUM.key(), hbaseCluster.getZookeeperQuorum());
        config.put(HbaseConfig.ROWKEY_COLUMNS.key(), "id");
        config.put(HbaseConfig.FAMILY_NAME.key(), Maps.of("all_columns", FAMILY_NAME));
        config.put(HbaseConfig.TABLE.key(), TABLE_NAME);
        // config.put(HbaseConfig.)

        catalog =
                new HbaseCatalog(
                        "hbase",
                        "default",
                        HbaseParameters.buildWithConfig(ReadonlyConfig.fromMap(config)));
        catalog.open();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (Objects.nonNull(admin)) {
            admin.close();
        }
        if (Objects.nonNull(catalog)) {
            catalog.close();
        }
        hbaseCluster.stopService();
    }

    @TestTemplate
    public void testHbaseSink(TestContainer container) throws IOException, InterruptedException {
        deleteData(table);
        Container.ExecResult sinkExecResult = container.executeJob("/fake-to-hbase.conf");
        Assertions.assertEquals(0, sinkExecResult.getExitCode());
        ArrayList<Result> results = readData(table);
        Assertions.assertEquals(results.size(), 5);
        Container.ExecResult sourceExecResult = container.executeJob("/hbase-to-assert.conf");
        Assertions.assertEquals(0, sourceExecResult.getExitCode());
    }

    @TestTemplate
    public void testHbaseSinkWithErrorWhenDataExists(TestContainer container)
            throws IOException, InterruptedException {
        deleteData(table);
        insertData(table);
        Assertions.assertEquals(5, countData(table));
        Container.ExecResult execResult =
                container.executeJob("/fake_to_hbase_with_error_when_data_exists.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
    }

    @TestTemplate
    public void testHbaseSinkWithRecreateSchema(TestContainer container)
            throws IOException, InterruptedException {
        String tableName = "seatunnel_test_with_recreate_schema";
        TableName table = TableName.valueOf(tableName);
        dropTable(table);
        hbaseCluster.createTable(tableName, Arrays.asList("test_rs"));
        TableDescriptor descriptorBefore = hbaseConnection.getTable(table).getDescriptor();
        String[] familiesBefore =
                Arrays.stream(descriptorBefore.getColumnFamilies())
                        .map(f -> f.getNameAsString())
                        .toArray(String[]::new);
        Assertions.assertTrue(Arrays.equals(familiesBefore, new String[] {"test_rs"}));
        Container.ExecResult execResult =
                container.executeJob("/fake_to_hbase_with_recreate_schema.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        TableDescriptor descriptorAfter = hbaseConnection.getTable(table).getDescriptor();
        String[] familiesAfter =
                Arrays.stream(descriptorAfter.getColumnFamilies())
                        .map(f -> f.getNameAsString())
                        .toArray(String[]::new);
        Assertions.assertTrue(!Arrays.equals(familiesBefore, familiesAfter));
    }

    @TestTemplate
    public void testHbaseSinkWithDropData(TestContainer container)
            throws IOException, InterruptedException {
        deleteData(table);
        insertData(table);
        countData(table);
        Assertions.assertEquals(5, countData(table));
        Container.ExecResult execResult =
                container.executeJob("/fake_to_hbase_with_drop_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(5, countData(table));
    }

    @TestTemplate
    public void testHbaseSinkWithCreateWhenNotExists(TestContainer container)
            throws IOException, InterruptedException {
        TableName seatunnelTestWithCreateWhenNotExists =
                TableName.valueOf("seatunnel_test_with_create_when_not_exists");
        dropTable(seatunnelTestWithCreateWhenNotExists);
        Container.ExecResult execResult =
                container.executeJob("/fake_to_hbase_with_create_when_not_exists.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(5, countData(seatunnelTestWithCreateWhenNotExists));
    }

    @TestTemplate
    public void testHbaseSinkWithAppendData(TestContainer container)
            throws IOException, InterruptedException {
        deleteData(table);
        insertData(table);
        countData(table);
        Assertions.assertEquals(5, countData(table));
        Container.ExecResult execResult =
                container.executeJob("/fake_to_hbase_with_append_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(10, countData(table));
    }

    @TestTemplate
    public void testHbaseSinkWithErrorWhenNotExists(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake_to_hbase_with_error_when_not_exists.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
    }

    @TestTemplate
    public void testHbaseSinkWithArray(TestContainer container)
            throws IOException, InterruptedException {
        deleteData(table);
        Container.ExecResult execResult = container.executeJob("/fake-to-hbase-array.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        ArrayList<Result> results = new ArrayList<>();
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            for (Cell cell : result.listCells()) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if ("A".equals(rowKey) && "info:c_array_string".equals(columnName)) {
                    Assertions.assertEquals(value, "\"a\",\"b\",\"c\"");
                }
                if ("B".equals(rowKey) && "info:c_array_int".equals(columnName)) {
                    Assertions.assertEquals(value, "4,5,6");
                }
            }
            results.add(result);
        }
        Assertions.assertEquals(results.size(), 3);
        scanner.close();
    }

    @TestTemplate
    public void testHbaseSinkAssignCfSink(TestContainer container)
            throws IOException, InterruptedException {
        deleteData(tableAssign);

        Container.ExecResult sinkExecResult = container.executeJob("/fake-to-assign-cf-hbase.conf");
        Assertions.assertEquals(0, sinkExecResult.getExitCode());

        Table hbaseTable = hbaseConnection.getTable(tableAssign);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        ArrayList<Result> results = new ArrayList<>();
        for (Result result : scanner) {
            results.add(result);
        }

        Assertions.assertEquals(results.size(), 5);

        if (scanner != null) {
            scanner.close();
        }
        int cf1Count = 0;
        int cf2Count = 0;

        for (Result result : results) {
            for (Cell cell : result.listCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                if ("cf1".equals(family)) {
                    cf1Count++;
                }
                if ("cf2".equals(family)) {
                    cf2Count++;
                }
            }
        }
        // check cf1 and cf2
        Assertions.assertEquals(cf1Count, 5);
        Assertions.assertEquals(cf2Count, 5);
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK},
            disabledReason = "Currently FLINK does not support multiple table write")
    public void testHbaseMultiTableSink(TestContainer container)
            throws IOException, InterruptedException {
        TableName multiTable1 = TableName.valueOf(MULTI_TABLE_ONE_NAME);
        TableName multiTable2 = TableName.valueOf(MULTI_TABLE_TWO_NAME);
        deleteData(multiTable1);
        deleteData(multiTable2);
        Container.ExecResult sinkExecResult =
                container.executeJob("/fake-to-hbase-with-multipletable.conf");
        Assertions.assertEquals(0, sinkExecResult.getExitCode());
        ArrayList<Result> results = readData(multiTable1);
        Assertions.assertEquals(results.size(), 1);
        results = readData(multiTable2);
        Assertions.assertEquals(results.size(), 1);
    }

    @TestTemplate
    public void testHbaseSourceWithBatchQuery(TestContainer container)
            throws IOException, InterruptedException {
        fakeToHbase(container);
        Container.ExecResult sourceExecResult =
                container.executeJob("/hbase-source-to-assert-with-batch-query.conf");
        Assertions.assertEquals(0, sourceExecResult.getExitCode());
    }

    @TestTemplate
    public void testCatalog(TestContainer container) {
        // create exiting table
        Assertions.assertThrows(
                TableAlreadyExistException.class,
                () -> catalog.createTable(TablePath.of("", "", TABLE_NAME), null, false));
        Assertions.assertDoesNotThrow(
                () -> catalog.createTable(TablePath.of("", "", TABLE_NAME), null, true));
        // drop table
        Assertions.assertDoesNotThrow(
                () -> catalog.createTable(TablePath.of("", "", "tmp"), null, false));
        Assertions.assertDoesNotThrow(() -> catalog.dropTable(TablePath.of("", "", "tmp"), false));
        Assertions.assertThrows(
                TableNotExistException.class,
                () -> catalog.dropTable(TablePath.of("", "", "tmp"), false));
    }

    private void fakeToHbase(TestContainer container) throws IOException, InterruptedException {
        deleteData(table);
        Container.ExecResult sinkExecResult = container.executeJob("/fake-to-hbase.conf");
        Assertions.assertEquals(0, sinkExecResult.getExitCode());
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        ArrayList<Result> results = new ArrayList<>();
        for (Result result : scanner) {
            results.add(result);
        }
        Assertions.assertEquals(results.size(), 5);
        scanner.close();
    }

    private void dropTable(TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    private void deleteData(TableName table) throws IOException {
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        // Delete the data generated by the test
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Delete deleteRow = new Delete(result.getRow());
            hbaseTable.delete(deleteRow);
        }
    }

    private void insertData(TableName table) throws IOException {
        Table hbaseTable = hbaseConnection.getTable(table);
        for (int i = 0; i < 5; i++) {
            String rowKey = "row" + UUID.randomUUID();
            String value = "value" + i;
            hbaseTable.put(
                    new Put(Bytes.toBytes(rowKey))
                            .addColumn(
                                    Bytes.toBytes(FAMILY_NAME),
                                    Bytes.toBytes("name"),
                                    Bytes.toBytes(value)));
        }
    }

    private int countData(TableName table) throws IOException {
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        int count = 0;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            count++;
        }
        scanner.close();
        return count;
    }

    public ArrayList<Result> readData(TableName table) throws IOException {
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        ArrayList<Result> results = new ArrayList<>();
        for (Result result : scanner) {
            results.add(result);
        }
        scanner.close();
        return results;
    }
}
