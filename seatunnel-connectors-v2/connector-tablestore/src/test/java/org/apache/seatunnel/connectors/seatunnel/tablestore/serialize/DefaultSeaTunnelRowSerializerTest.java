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

package org.apache.seatunnel.connectors.seatunnel.tablestore.serialize;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.RowPutChange;

import java.math.BigDecimal;
import java.util.Arrays;

public class DefaultSeaTunnelRowSerializerTest {

    private static TableStoreConfig config(String... primaryKeys) {
        TableStoreConfig config = new TableStoreConfig();
        config.setTable("test_table");
        config.setPrimaryKeys(Arrays.asList(primaryKeys));
        return config;
    }

    private static ColumnValue columnValue(RowPutChange change, String name) {
        for (Column column : change.getColumnsToPut()) {
            if (column.getName().equals(name)) {
                return column.getValue();
            }
        }
        throw new AssertionError("column not found: " + name);
    }

    @Test
    public void shouldSerializeIntegerFamilyColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"pk", "tiny", "small", "int_col", "bigint"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE
                        });
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"key-1", (byte) 1, (short) 2, 3, 4L});
        RowPutChange change =
                new DefaultSeaTunnelRowSerializer(rowType, config("pk")).serialize(row);

        Assertions.assertEquals(1L, columnValue(change, "tiny").asLong());
        Assertions.assertEquals(2L, columnValue(change, "small").asLong());
        Assertions.assertEquals(3L, columnValue(change, "int_col").asLong());
        Assertions.assertEquals(4L, columnValue(change, "bigint").asLong());
    }

    @Test
    public void shouldSerializeFloatingPointColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"pk", "float_col", "double_col", "decimal_col"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(10, 2)
                        });
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {"key-1", 1.5f, 2.25d, new BigDecimal("3.75")});
        RowPutChange change =
                new DefaultSeaTunnelRowSerializer(rowType, config("pk")).serialize(row);

        Assertions.assertEquals(1.5d, columnValue(change, "float_col").asDouble(), 0.0001d);
        Assertions.assertEquals(2.25d, columnValue(change, "double_col").asDouble(), 0.0001d);
        Assertions.assertEquals(3.75d, columnValue(change, "decimal_col").asDouble(), 0.0001d);
    }

    @Test
    public void shouldSerializeIntegerPrimaryKey() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {42, "alice"});
        RowPutChange change =
                new DefaultSeaTunnelRowSerializer(rowType, config("id")).serialize(row);

        PrimaryKeyColumn[] primaryKeyColumns = change.getPrimaryKey().getPrimaryKeyColumns();
        Assertions.assertEquals(1, primaryKeyColumns.length);
        Assertions.assertEquals("id", primaryKeyColumns[0].getName());
        Assertions.assertEquals(42L, primaryKeyColumns[0].getValue().asLong());
    }

    @Test
    public void shouldSerializeStringPrimaryKeyAndBaselineColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "flag", "payload"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            PrimitiveByteArrayType.INSTANCE
                        });
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {"key-1", "alice", true, new byte[] {1, 2}});
        RowPutChange change =
                new DefaultSeaTunnelRowSerializer(rowType, config("id")).serialize(row);

        Assertions.assertEquals(
                "key-1", change.getPrimaryKey().getPrimaryKeyColumns()[0].getValue().asString());
        Assertions.assertEquals("alice", columnValue(change, "name").asString());
        Assertions.assertTrue(columnValue(change, "flag").asBoolean());
        Assertions.assertArrayEquals(new byte[] {1, 2}, columnValue(change, "payload").asBinary());
    }

    @Test
    public void shouldWriteToConfiguredTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        TableStoreConfig tableStoreConfig = config("id");
        tableStoreConfig.setTable("my_table");
        RowPutChange change =
                new DefaultSeaTunnelRowSerializer(rowType, tableStoreConfig)
                        .serialize(new SeaTunnelRow(new Object[] {"k"}));

        Assertions.assertEquals("my_table", change.getTableName());
    }
}
