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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

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
@DisabledOnContainer(
        value = {},
        type = {EngineType.SEATUNNEL},
        disabledReason = "The hbase container authentication configuration is incorrect.")
public class HbaseIT extends TestSuiteBase implements TestResource {

    private static final String TABLE_NAME = "seatunnel_test";
    private static final String ASSIGN_CF_TABLE_NAME = "assign_cf_table";

    private static final String FAMILY_NAME = "info";

    private Connection hbaseConnection;

    private Admin admin;

    private TableName table;
    private TableName tableAssign;

    private HbaseCluster hbaseCluster;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hbaseCluster = new HbaseCluster();
        hbaseConnection = hbaseCluster.startService();
        // Create table for hbase sink test
        log.info("initial");
        hbaseCluster.createTable(TABLE_NAME, Arrays.asList(FAMILY_NAME));
        hbaseCluster.createTable(ASSIGN_CF_TABLE_NAME, Arrays.asList("cf1", "cf2"));
        table = TableName.valueOf(TABLE_NAME);
        tableAssign = TableName.valueOf(ASSIGN_CF_TABLE_NAME);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (Objects.nonNull(admin)) {
            admin.close();
        }
        hbaseCluster.stopService();
    }

    @TestTemplate
    public void testHbaseSink(TestContainer container) throws IOException, InterruptedException {
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
        Container.ExecResult sourceExecResult = container.executeJob("/hbase-to-assert.conf");
        Assertions.assertEquals(0, sourceExecResult.getExitCode());
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
}
