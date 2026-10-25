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

package org.apache.seatunnel.e2e.connector.fluss;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.CloseableIterator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class FlussSinkIT extends FlussTestBase {

    private static final String DB_NAME = "fluss_db_test";
    private static final String DB_NAME_2 = "fluss_db_test2";
    private static final String DB_NAME_3 = "fluss_db_test3";
    private static final String TABLE_NAME = "fluss_tb_table1";
    private static final String TABLE_NAME_2 = "fluss_tb_table2";
    private static final String TABLE_NAME_3 = "fluss_tb_table3";

    @TestTemplate
    public void testFlussSink(TestContainer container) throws Exception {
        log.info(" create fluss table");
        createDb(flussConnection, DB_NAME);
        createTable(flussConnection, DB_NAME, TABLE_NAME, getFlussSchema());
        Container.ExecResult execFake2fluss = container.executeJob("/fake_to_fluss.conf");
        Assertions.assertEquals(0, execFake2fluss.getExitCode(), execFake2fluss.getStderr());
        checkFlussData(DB_NAME, TABLE_NAME);
    }

    @TestTemplate
    public void testFlussMultiTableSink(TestContainer container) throws Exception {
        log.info(" create fluss tables");
        createDb(flussConnection, DB_NAME_2);
        createDb(flussConnection, DB_NAME_3);
        createTable(flussConnection, DB_NAME_2, TABLE_NAME, getFlussSchema());
        createTable(flussConnection, DB_NAME_2, TABLE_NAME_2, getFlussSchema());
        createTable(flussConnection, DB_NAME_3, TABLE_NAME_3, getFlussSchema());

        Container.ExecResult execFake2fluss =
                container.executeJob("/fake_to_multipletable_fluss.conf");
        Assertions.assertEquals(0, execFake2fluss.getExitCode(), execFake2fluss.getStderr());
        checkFlussData(DB_NAME_2, TABLE_NAME);
        checkFlussData(DB_NAME_2, TABLE_NAME_2);
        checkFlussData(DB_NAME_3, TABLE_NAME_3);
    }

    public void checkFlussData(String dbName, String tableName) throws IOException {
        // check log data
        List<GenericRow> streamData =
                getFlussTableStreamData(flussConnection, dbName, tableName, 10);
        checkFlussTableStreamData(streamData);
        // check data
        List<GenericRow> data = getFlussTableData(flussConnection, dbName, tableName, 10);
        checkFlussTableData(data);
    }

    public void checkFlussTableData(List<GenericRow> streamData) {
        Assertions.assertEquals(3, streamData.size());
        List<String> expectedResult =
                Arrays.asList(
                        "([109, 105, 73, 90, 106],true,1940337748,73,17489,7408919466156976747,9.434991E37,3.140411637757371E307,4029933791018936000000.00000000,aaaaa,20091,9010000,2025-05-27T21:56:09,2025-09-27T18:54:08Z)",
                        "([109, 105, 73, 90, 106],true,90650390,37,22504,5851888708829345169,2.6221706E36,1.8915341983748786E307,3093109630614623000000.00000000,bbbbb,20089,76964000,2025-05-08T05:26:18,2025-08-04T08:49:45Z)",
                        "([109, 105, 73, 90, 106],true,388742243,89,15831,159071788675312856,7.310445E37,1.2166972324288247E308,7994947075691901000000.00000000,ddddd,20092,55687000,2025-07-18T08:59:49,2025-09-12T15:46:25Z)");
        ArrayList<String> result = new ArrayList<>();
        for (GenericRow streamDatum : streamData) {
            result.add(streamDatum.toString());
        }
        Assertions.assertEquals(expectedResult, result);
    }

    public void checkFlussTableStreamData(List<GenericRow> streamData) {
        Assertions.assertEquals(7, streamData.size());
        List<String> expectedResult =
                Arrays.asList(
                        "([109, 105, 73, 90, 106],true,1940337748,73,17489,7408919466156976747,9.434991E37,3.140411637757371E307,4029933791018936000000.00000000,aaaaa,20091,9010000,2025-05-27T21:56:09,2025-09-27T18:54:08Z)",
                        "([109, 105, 73, 90, 106],true,90650390,37,22504,5851888708829345169,2.6221706E36,1.8915341983748786E307,3093109630614623000000.00000000,bbbbb,20089,76964000,2025-05-08T05:26:18,2025-08-04T08:49:45Z)",
                        "([109, 105, 73, 90, 106],true,2146418323,79,19821,6393905306944584839,2.0462337E38,1.4868114385836557E308,5594947262031770000000.00000000,ccccc,20367,79840000,2025-03-25T01:49:14,2025-07-03T03:52:06Z)",
                        "([109, 105, 73, 90, 106],true,2146418323,79,19821,6393905306944584839,2.0462337E38,1.4868114385836557E308,5594947262031770000000.00000000,ccccc,20367,79840000,2025-03-25T01:49:14,2025-07-03T03:52:06Z)",
                        "([109, 105, 73, 90, 106],true,82794384,27,30339,5826566947079347516,2.2137477E37,1.7737681870839753E308,3984670873242882300000.00000000,ddddd,20344,37972000,2025-01-27T19:20:51,2025-11-06T18:38:54Z)",
                        "([109, 105, 73, 90, 106],true,82794384,27,30339,5826566947079347516,2.2137477E37,1.7737681870839753E308,3984670873242882300000.00000000,ddddd,20344,37972000,2025-01-27T19:20:51,2025-11-06T18:38:54Z)",
                        "([109, 105, 73, 90, 106],true,388742243,89,15831,159071788675312856,7.310445E37,1.2166972324288247E308,7994947075691901000000.00000000,ddddd,20092,55687000,2025-07-18T08:59:49,2025-09-12T15:46:25Z)");
        ArrayList<String> result = new ArrayList<>();
        for (GenericRow streamDatum : streamData) {
            result.add(streamDatum.toString());
        }
        Assertions.assertEquals(expectedResult, result);
    }

    public List<GenericRow> getFlussTableStreamData(
            Connection connection, String dbName, String tableName, int scanNum) {
        TablePath tablePath = TablePath.of(dbName, tableName);
        Table table = connection.getTable(tablePath);
        LogScanner logScanner = table.newScan().createLogScanner();
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }
        int scanned = 0;
        List<GenericRow> rows = new ArrayList<>();

        while (true) {
            if (scanned > scanNum) break;
            log.info("Polling for stream records...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    GenericRow row = (GenericRow) record.getRow();
                    rows.add(row);
                }
            }
            scanned++;
        }
        return rows;
    }

    public List<GenericRow> getFlussTableData(
            Connection connection, String dbName, String tableName, int scanNum)
            throws IOException {
        TablePath tablePath = TablePath.of(dbName, tableName);
        Table table = connection.getTable(tablePath);
        LogScanner logScanner = table.newScan().createLogScanner();
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }
        int scanned = 0;
        List<GenericRow> rows = new ArrayList<>();

        while (true) {
            if (scanned > scanNum) break;
            log.info("Polling for records...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                CloseableIterator<InternalRow> data =
                        table.newScan()
                                .limit(10)
                                .createBatchScanner(bucket)
                                .pollBatch(Duration.ofSeconds(5));
                while (data.hasNext()) {
                    rows.add((GenericRow) data.next());
                }
            }
            scanned++;
        }
        return rows;
    }
}
