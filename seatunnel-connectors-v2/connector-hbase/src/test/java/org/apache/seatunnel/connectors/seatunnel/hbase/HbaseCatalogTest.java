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

package org.apache.seatunnel.connectors.seatunnel.hbase;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.hbase.catalog.HbaseCatalog;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

public class HbaseCatalogTest {

    @Test
    public void testTableExistsWithNamespace() throws Exception {
        HbaseParameters parameters =
                HbaseParameters.builder()
                        .zookeeperQuorum("localhost")
                        .namespace("ns1")
                        .table("tbl")
                        .build();
        HbaseCatalog catalog = new HbaseCatalog("hbase", "ns1", parameters);

        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        Mockito.when(hbaseClient.tableExists("ns1:tbl")).thenReturn(true);

        injectHbaseClient(catalog, hbaseClient);

        TablePath tablePath = TablePath.of("ns1", "tbl");
        Assertions.assertTrue(catalog.tableExists(tablePath));
        Mockito.verify(hbaseClient, Mockito.times(1)).tableExists("ns1:tbl");
    }

    @Test
    public void testTableExistsWithoutNamespace() throws Exception {
        HbaseParameters parameters =
                HbaseParameters.builder()
                        .zookeeperQuorum("localhost")
                        .namespace("default")
                        .table("tbl")
                        .build();
        HbaseCatalog catalog = new HbaseCatalog("hbase", "default", parameters);

        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        Mockito.when(hbaseClient.tableExists("tbl")).thenReturn(true);

        injectHbaseClient(catalog, hbaseClient);

        TablePath tablePath = TablePath.of("tbl");
        Assertions.assertTrue(catalog.tableExists(tablePath));
        Mockito.verify(hbaseClient, Mockito.times(1)).tableExists("tbl");
    }

    private void injectHbaseClient(HbaseCatalog catalog, HbaseClient hbaseClient) throws Exception {
        Field clientField = HbaseCatalog.class.getDeclaredField("hbaseClient");
        clientField.setAccessible(true);
        clientField.set(catalog, hbaseClient);
    }
}
