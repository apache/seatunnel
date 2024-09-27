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

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Spark and Flink engine can not auto create paimon table on worker node in local file(e.g flink tm) by savemode feature which can lead error")
@Slf4j
public class PaimonStreamReadIT extends PaimonSinkCDCIT {

    @TestTemplate
    public void testStreamReadPaimon(TestContainer container) throws Exception {
        Container.ExecResult writeResult =
                container.executeJob("/fake_to_paimon_with_full_type.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());

        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/paimon_to_paimon.conf");
                    } catch (Exception e) {
                        throw new SeaTunnelException(e);
                    }
                });

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(400L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            container.executeExtraCommands(containerExtendedFactory);
                            List<PaimonRecordWithFullType> paimonSourceRecords =
                                    loadPaimonDataWithFullType("full_type", "st_test");
                            List<PaimonRecordWithFullType> paimonSinkRecords =
                                    loadPaimonDataWithFullType("full_type", "st_test_sink");
                            Assertions.assertEquals(
                                    paimonSourceRecords.size(), paimonSinkRecords.size());
                            Assertions.assertIterableEquals(paimonSourceRecords, paimonSinkRecords);
                        });

        // write cdc data
        Container.ExecResult writeResult1 =
                container.executeJob("/fake_to_paimon_with_full_type_cdc_data.conf");
        Assertions.assertEquals(0, writeResult1.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(400L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            container.executeExtraCommands(containerExtendedFactory);
                            List<PaimonRecordWithFullType> paimonSourceRecords =
                                    loadPaimonDataWithFullType("full_type", "st_test");
                            List<PaimonRecordWithFullType> paimonSinkRecords =
                                    loadPaimonDataWithFullType("full_type", "st_test_sink");
                            Assertions.assertEquals(
                                    paimonSourceRecords.size(), paimonSinkRecords.size());
                            Assertions.assertIterableEquals(paimonSourceRecords, paimonSinkRecords);
                        });
    }

    protected List<PaimonRecordWithFullType> loadPaimonDataWithFullType(
            String dbName, String tbName) {
        FileStoreTable table = (FileStoreTable) getTable(dbName, tbName);
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        TableRead tableRead = readBuilder.newRead();
        List<PaimonRecordWithFullType> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        InternalMap internalMap = row.getMap(0);
                        InternalArray keyArray = internalMap.keyArray();
                        InternalArray valueArray = internalMap.valueArray();
                        HashMap<Object, Object> map = new HashMap<>(internalMap.size());
                        for (int i = 0; i < internalMap.size(); i++) {
                            map.put(keyArray.getString(i), valueArray.getString(i));
                        }
                        InternalArray internalArray = row.getArray(1);
                        int[] intArray = internalArray.toIntArray();
                        PaimonRecordWithFullType paimonRecordWithFullType =
                                new PaimonRecordWithFullType(
                                        map,
                                        intArray,
                                        row.getString(2),
                                        row.getBoolean(3),
                                        row.getShort(4),
                                        row.getShort(5),
                                        row.getInt(6),
                                        row.getLong(7),
                                        row.getFloat(8),
                                        row.getDouble(9),
                                        row.getDecimal(10, 30, 8),
                                        row.getString(11),
                                        row.getInt(12),
                                        row.getTimestamp(13, 6));
                        result.add(paimonRecordWithFullType);
                    });
        } catch (IOException e) {
            throw new SeaTunnelException(e);
        }
        return result;
    }
}
