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

package org.apache.seatunnel.connectors.seatunnel.hbase.client;

import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

class HbaseClientTest {

    @Test
    void testIsExistsDataReturnsFalseWhenScannerNextReturnsNull() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Table table = Mockito.mock(Table.class);
        ResultScanner scanner = Mockito.mock(ResultScanner.class);
        Mockito.when(connection.getTable(any(TableName.class))).thenReturn(table);
        Mockito.when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        Mockito.when(scanner.next()).thenReturn(null);

        HbaseClient client = newHbaseClient(connection);

        assertFalse(client.isExistsData("ns", "tbl"));
    }

    @Test
    void testIsExistsDataReturnsTrueWhenScannerHasResult() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Table table = Mockito.mock(Table.class);
        ResultScanner scanner = Mockito.mock(ResultScanner.class);
        Result result = Mockito.mock(Result.class);
        Mockito.when(result.isEmpty()).thenReturn(false);
        Mockito.when(connection.getTable(any(TableName.class))).thenReturn(table);
        Mockito.when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        Mockito.when(scanner.next()).thenReturn(result);

        HbaseClient client = newHbaseClient(connection);

        assertTrue(client.isExistsData("ns", "tbl"));
    }

    private HbaseClient newHbaseClient(Connection connection) throws Exception {
        HbaseClient.hbaseConfiguration = HBaseConfiguration.create();
        Mockito.when(connection.getAdmin()).thenReturn(Mockito.mock(Admin.class));
        Mockito.when(connection.getBufferedMutator(any(BufferedMutatorParams.class)))
                .thenReturn(Mockito.mock(BufferedMutator.class));
        HbaseParameters hbaseParameters = Mockito.mock(HbaseParameters.class);
        Mockito.when(hbaseParameters.getNamespace()).thenReturn("ns");
        Mockito.when(hbaseParameters.getTable()).thenReturn("tbl");
        Mockito.when(hbaseParameters.getWriteBufferSize()).thenReturn(1);

        Constructor<HbaseClient> constructor =
                HbaseClient.class.getDeclaredConstructor(Connection.class, HbaseParameters.class);
        constructor.setAccessible(true);
        return constructor.newInstance(connection, hbaseParameters);
    }
}
