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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
import java.util.Objects;

@Slf4j
// @DisabledOnContainer(
//        value = {},
//        type = {EngineType.SEATUNNEL},
//        disabledReason =
//                "After the Zeta engine finishes syncing, hbase thread pool threads remain in the
// container. To prevent errors, it's disabled.")
public class HbaseIT extends TestSuiteBase implements TestResource {

    private static final String TABLE_NAME = "seatunnel_test";

    private static final String FAMILY_NAME = "info";

    private Connection hbaseConnection;

    private Admin admin;

    private TableName table;

    private HbaseCluster hbaseCluster;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hbaseCluster = new HbaseCluster();
        hbaseConnection = hbaseCluster.startService();
        this.initialize();
        table = TableName.valueOf(TABLE_NAME);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (Objects.nonNull(admin)) {
            admin.close();
        }
        hbaseCluster.stopService();
    }

    private void initialize() throws IOException {
        // Create table for hbase sink test
        log.info("initial");
        System.out.println("initial");
        hbaseCluster.createTable(TABLE_NAME, Arrays.asList(FAMILY_NAME));
        // Create table and insert data for hbase source test
        hbaseCluster.createTable("test_table", Arrays.asList("cf1", "cf2"));
        hbaseCluster.putRow("test_table", "row1", "cf1", "col1", "True"); // boolean
        hbaseCluster.putRow("test_table", "row1", "cf1", "col2", "3.923"); // double
        hbaseCluster.putRow("test_table", "row1", "cf2", "col1", "4537654375638"); // bigint
        hbaseCluster.putRow("test_table", "row1", "cf2", "col2", "33"); // int
        hbaseCluster.putRow("test_table", "row1", "cf2", "col_date", "2024-08-12"); // date
        hbaseCluster.putRow(
                "test_table", "row1", "cf2", "col_timestamp", "2024-08-27 03:15:10"); // timestamp
        hbaseCluster.putRow("test_table", "row1", "cf2", "col_time", "04:44:09"); // time
        hbaseCluster.putRow("test_table", "row2", "cf1", "col1", "False"); // boolean
        hbaseCluster.putRow("test_table", "row2", "cf1", "col2", "465.573"); // double
        hbaseCluster.putRow("test_table", "row2", "cf2", "col1", "7893658245220"); // bigint
        hbaseCluster.putRow("test_table", "row2", "cf2", "col2", "31"); // int
        hbaseCluster.putRow("test_table", "row2", "cf2", "col_date", "2024-04-03"); // date
        hbaseCluster.putRow(
                "test_table", "row2", "cf2", "col_timestamp", "2024-11-22 10:41:49"); // timestamp
        hbaseCluster.putRow("test_table", "row2", "cf2", "col_time", "14:24:38"); // time
        System.out.println("Hbase table has been initialized");
    }

    @TestTemplate
    public void testHbaseSink(TestContainer container) throws IOException, InterruptedException {
        System.out.println("sink begin");
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        // Delete the data generated by the test
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Delete deleteRow = new Delete(result.getRow());
            hbaseTable.delete(deleteRow);
        }

        Container.ExecResult execResult = container.executeJob("/fake-to-hbase.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        scanner = hbaseTable.getScanner(scan);
        ArrayList<Result> results = new ArrayList<>();
        for (Result result : scanner) {
            results.add(result);
        }
        Assertions.assertEquals(results.size(), 5);
        scanner.close();
        System.out.println("sink end");
        System.out.println("source begin,data:");
        scan = new Scan();
        scanner = hbaseTable.getScanner(scan);
        // Delete the data generated by the test
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            System.out.println(result.getRow());
        }
        System.out.println("source data end");
        Container.ExecResult exec = container.executeJob("/hbase1-to-assert.conf");
        Assertions.assertEquals(0, exec.getExitCode());
        System.out.println("source end");
    }

    @TestTemplate
    public void testHbaseSource(TestContainer container) throws IOException, InterruptedException {
        System.out.println("source begin,data:");
        Table hbaseTable = hbaseConnection.getTable(TableName.valueOf("test_table"));
        Scan scan = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        // Delete the data generated by the test
        for (Result result : scanner) {
            // 获取行键
            byte[] row = result.getRow();
            String rowStr = Bytes.toString(row);

            // 打印行键
            System.out.println("Row: " + rowStr);

            // 遍历该行的所有列
            for (Cell cell : result.listCells()) {
                // 获取列族
                byte[] family = CellUtil.cloneFamily(cell);
                String familyStr = Bytes.toString(family);

                // 获取列限定符
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                String qualifierStr = Bytes.toString(qualifier);

                // 获取值
                byte[] value = CellUtil.cloneValue(cell);
                String valueStr = Bytes.toString(value);

                // 打印列族、列限定符和值
                System.out.println(
                        "Family: "
                                + familyStr
                                + ", Qualifier: "
                                + qualifierStr
                                + ", Value: "
                                + valueStr);
            }
        }
        System.out.println("source data end");
        Container.ExecResult execResult = container.executeJob("/hbase-to-assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        System.out.println("source end");
    }

    @TestTemplate
    public void testHbaseSinkWithArray(TestContainer container)
            throws IOException, InterruptedException {
        System.out.println("array sink begin");
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ArrayList<Result> results = new ArrayList<>();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        // Delete the data generated by the test
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Delete deleteRow = new Delete(result.getRow());
            hbaseTable.delete(deleteRow);
        }

        Container.ExecResult execResult = container.executeJob("/fake-to-hbase-array.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        scanner = hbaseTable.getScanner(scan);
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
        System.out.println("sink end");
    }
}
