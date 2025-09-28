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

package org.apache.seatunnel.connectors.seatunnel.databend.sink;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabendSinkWriterTest {

    @Test
    public void testGenerateMergeSql() throws Exception {
        // Create a mock DatabendSinkWriter
        DatabendSinkWriter sinkWriter = mock(DatabendSinkWriter.class);

        // Set up the real method to test
        when(sinkWriter.generateMergeSql()).thenCallRealMethod();

        // Use reflection to set private fields
        setPrivateField(sinkWriter, "conflictKey", "id");
        setPrivateField(sinkWriter, "sinkTablePath", TablePath.of("test_db", "target_table"));
        setPrivateField(sinkWriter, "streamName", "cdc_stream");
        setPrivateField(sinkWriter, "enableDelete", true);
        setPrivateField(sinkWriter, "targetTableName", "target_table");

        // Mock catalogTable
        org.apache.seatunnel.api.table.catalog.CatalogTable catalogTable =
                mock(org.apache.seatunnel.api.table.catalog.CatalogTable.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE
                        });
        when(catalogTable.getSeaTunnelRowType()).thenReturn(rowType);
        setPrivateField(sinkWriter, "catalogTable", catalogTable);

        // Call the method
        String mergeSql = sinkWriter.generateMergeSql();

        // Expected SQL
        String expectedSql =
                "MERGE INTO test_db.target_table a "
                        + "USING (SELECT raw_data:id as id, raw_data:name as name, raw_data:score as score, action "
                        + "FROM test_db.cdc_stream "
                        + "QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY add_time DESC) = 1) b "
                        + "ON a.id = b.id "
                        + "WHEN MATCHED AND b.action = 'update' THEN UPDATE * "
                        + "WHEN MATCHED AND b.action = 'delete' THEN DELETE "
                        + "WHEN NOT MATCHED AND b.action!='delete' THEN INSERT *";

        assertEquals(expectedSql, mergeSql);
    }

    @Test
    public void testGenerateMergeSqlWithoutDelete() throws Exception {
        // Create a mock DatabendSinkWriter
        DatabendSinkWriter sinkWriter = mock(DatabendSinkWriter.class);

        // Set up the real method to test
        when(sinkWriter.generateMergeSql()).thenCallRealMethod();

        // Use reflection to set private fields
        setPrivateField(sinkWriter, "conflictKey", "id");
        setPrivateField(sinkWriter, "sinkTablePath", TablePath.of("test_db", "target_table"));
        setPrivateField(sinkWriter, "streamName", "cdc_stream");
        setPrivateField(sinkWriter, "enableDelete", false);
        setPrivateField(sinkWriter, "targetTableName", "target_table");

        // Mock catalogTable
        org.apache.seatunnel.api.table.catalog.CatalogTable catalogTable =
                mock(org.apache.seatunnel.api.table.catalog.CatalogTable.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE
                        });
        when(catalogTable.getSeaTunnelRowType()).thenReturn(rowType);
        setPrivateField(sinkWriter, "catalogTable", catalogTable);

        // Call the method
        String mergeSql = sinkWriter.generateMergeSql();

        // Expected SQL without DELETE clause
        String expectedSql =
                "MERGE INTO test_db.target_table a "
                        + "USING (SELECT raw_data:id as id, raw_data:name as name, raw_data:score as score, action "
                        + "FROM test_db.cdc_stream "
                        + "QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY add_time DESC) = 1) b "
                        + "ON a.id = b.id "
                        + "WHEN MATCHED AND b.action = 'update' THEN UPDATE * "
                        + "WHEN NOT MATCHED AND b.action!='delete' THEN INSERT *";

        assertEquals(expectedSql, mergeSql);
    }

    @Test
    public void testGetConflictKeyValue() throws Exception {
        // Create a mock DatabendSinkWriter
        DatabendSinkWriter sinkWriter = mock(DatabendSinkWriter.class);

        // Get the method to test
        Method method =
                DatabendSinkWriter.class.getDeclaredMethod(
                        "getConflictKeyValue", SeaTunnelRow.class);
        method.setAccessible(true);

        // Mock catalogTable
        org.apache.seatunnel.api.table.catalog.CatalogTable catalogTable =
                mock(org.apache.seatunnel.api.table.catalog.CatalogTable.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE
                        });
        when(catalogTable.getSeaTunnelRowType()).thenReturn(rowType);
        setPrivateField(sinkWriter, "catalogTable", catalogTable);

        // Create test row
        Object[] fields = {1, "test", 95.5};
        SeaTunnelRow row = new SeaTunnelRow(fields);

        // Set conflict key
        setPrivateField(sinkWriter, "conflictKey", "id");

        // Call the method
        String conflictKeyValue = (String) method.invoke(sinkWriter, row);

        // Expected value - should be 1
        assertEquals("1", conflictKeyValue);
    }

    @Test
    public void testGetConflictKeyValueWithNullValue() throws Exception {
        // Create a mock DatabendSinkWriter
        DatabendSinkWriter sinkWriter = mock(DatabendSinkWriter.class);

        // Get the method to test
        Method method =
                DatabendSinkWriter.class.getDeclaredMethod(
                        "getConflictKeyValue", SeaTunnelRow.class);
        method.setAccessible(true);

        // Mock catalogTable
        org.apache.seatunnel.api.table.catalog.CatalogTable catalogTable =
                mock(org.apache.seatunnel.api.table.catalog.CatalogTable.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE
                        });
        when(catalogTable.getSeaTunnelRowType()).thenReturn(rowType);
        setPrivateField(sinkWriter, "catalogTable", catalogTable);

        // Create test row with null conflict key value
        Object[] fields = {null, "test", 95.5};
        SeaTunnelRow row = new SeaTunnelRow(fields);

        // Set conflict key
        setPrivateField(sinkWriter, "conflictKey", "id");

        // Call the method - should throw IllegalArgumentException wrapped in
        // InvocationTargetException
        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () -> {
                            method.invoke(sinkWriter, row);
                        });

        // Verify the cause is IllegalArgumentException
        assertEquals(IllegalArgumentException.class, exception.getCause().getClass());
    }

    // Helper method to set private fields using reflection
    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
