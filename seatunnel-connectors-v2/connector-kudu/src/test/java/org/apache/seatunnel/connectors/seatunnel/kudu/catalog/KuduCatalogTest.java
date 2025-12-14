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

package org.apache.seatunnel.connectors.seatunnel.kudu.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;

class KuduCatalogTest {

    @Test
    void testStringColumnLengthShouldBeNull() throws Exception {
        CommonConfig commonConfig = Mockito.mock(CommonConfig.class);
        KuduCatalog kuduCatalog = new KuduCatalog("kudu", commonConfig);

        KuduClient kuduClient = Mockito.mock(KuduClient.class);
        Field clientField = KuduCatalog.class.getDeclaredField("kuduClient");
        clientField.setAccessible(true);
        clientField.set(kuduCatalog, kuduClient);

        TablePath tablePath = TablePath.of("kudu_string_table");
        Mockito.when(kuduClient.tableExists(tablePath.getFullName())).thenReturn(true);

        ColumnSchema idColumn =
                new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build();
        ColumnSchema stringColumn =
                new ColumnSchema.ColumnSchemaBuilder("val_string", Type.STRING)
                        .nullable(true)
                        .build();
        Schema schema = new Schema(Arrays.asList(idColumn, stringColumn));

        KuduTable kuduTable = Mockito.mock(KuduTable.class);
        Mockito.when(kuduClient.openTable(tablePath.getFullName())).thenReturn(kuduTable);
        Mockito.when(kuduTable.getSchema()).thenReturn(schema);
        Mockito.when(kuduTable.getPartitionSchema()).thenReturn(null);

        CatalogTable catalogTable = kuduCatalog.getTable(tablePath);
        Column id = catalogTable.getTableSchema().getColumns().get(0);
        Column valString = catalogTable.getTableSchema().getColumns().get(1);

        // Non-STRING types should still keep the physical length from Kudu.
        Assertions.assertEquals("id", id.getName());
        Assertions.assertNotNull(id.getColumnLength());

        // STRING columns must not use the internal typeSize (commonly 16) as logical length.
        Assertions.assertEquals("val_string", valString.getName());
        Assertions.assertNull(valString.getColumnLength());
    }
}
