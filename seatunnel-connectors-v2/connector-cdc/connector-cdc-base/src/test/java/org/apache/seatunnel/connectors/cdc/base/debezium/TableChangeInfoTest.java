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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for TableChangeInfo */
public class TableChangeInfoTest {

    @Test
    public void testConstructorAndGetters() {
        String tableId = "db.schema.table";
        TableChangeInfo.TableChangeType changeType = TableChangeInfo.TableChangeType.CREATE;
        byte[] schema = "{\"type\":\"table\"}".getBytes();

        TableChangeInfo info = new TableChangeInfo(tableId, changeType, schema);

        assertEquals(tableId, info.getTableId());
        assertEquals(changeType, info.getChangeType());
        assertArrayEquals(schema, info.getSerializedTableSchema());
    }

    @Test
    public void testTableChangeTypes() {
        // Verify all enum values exist
        assertEquals(3, TableChangeInfo.TableChangeType.values().length);
        assertNotNull(TableChangeInfo.TableChangeType.CREATE);
        assertNotNull(TableChangeInfo.TableChangeType.ALTER);
        assertNotNull(TableChangeInfo.TableChangeType.DROP);
    }

    @Test
    public void testEqualsAndHashCode() {
        String tableId = "db.schema.table";
        TableChangeInfo.TableChangeType changeType = TableChangeInfo.TableChangeType.CREATE;
        byte[] schema1 = "{\"type\":\"table\"}".getBytes();
        byte[] schema2 = "{\"type\":\"different\"}".getBytes();

        TableChangeInfo info1 = new TableChangeInfo(tableId, changeType, schema1);
        TableChangeInfo info2 = new TableChangeInfo(tableId, changeType, schema2);
        TableChangeInfo info3 = new TableChangeInfo("different.table", changeType, schema1);

        // Same tableId and changeType should be equal
        assertEquals(info1, info2);
        assertEquals(info1.hashCode(), info2.hashCode());

        // Different tableId should not be equal
        assertNotEquals(info1, info3);
    }

    @Test
    public void testEqualsSameObject() {
        TableChangeInfo info =
                new TableChangeInfo("table", TableChangeInfo.TableChangeType.CREATE, new byte[0]);
        assertEquals(info, info);
    }

    @Test
    public void testEqualsNull() {
        TableChangeInfo info =
                new TableChangeInfo("table", TableChangeInfo.TableChangeType.CREATE, new byte[0]);
        assertNotEquals(info, null);
    }

    @Test
    public void testEqualsDifferentClass() {
        TableChangeInfo info =
                new TableChangeInfo("table", TableChangeInfo.TableChangeType.CREATE, new byte[0]);
        assertNotEquals(info, "not a TableChangeInfo");
    }

    @Test
    public void testToString() {
        String tableId = "db.schema.table";
        TableChangeInfo.TableChangeType changeType = TableChangeInfo.TableChangeType.ALTER;
        byte[] schema = new byte[100];

        TableChangeInfo info = new TableChangeInfo(tableId, changeType, schema);
        String str = info.toString();

        assertTrue(str.contains("TableChangeInfo"));
        assertTrue(str.contains(tableId));
        assertTrue(str.contains("ALTER"));
        assertTrue(str.contains("100"));
    }

    @Test
    public void testToStringWithNullSchema() {
        TableChangeInfo info =
                new TableChangeInfo("table", TableChangeInfo.TableChangeType.DROP, null);
        String str = info.toString();

        assertTrue(str.contains("TableChangeInfo"));
        assertTrue(str.contains("DROP"));
        assertTrue(str.contains("0"));
    }

    @Test
    public void testSerialization() throws Exception {
        String tableId = "db.schema.table";
        TableChangeInfo.TableChangeType changeType = TableChangeInfo.TableChangeType.CREATE;
        byte[] schema = "{\"columns\":[]}".getBytes();

        TableChangeInfo original = new TableChangeInfo(tableId, changeType, schema);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        TableChangeInfo deserialized = (TableChangeInfo) ois.readObject();
        ois.close();

        // Verify
        assertEquals(original, deserialized);
        assertEquals(original.getTableId(), deserialized.getTableId());
        assertEquals(original.getChangeType(), deserialized.getChangeType());
        assertArrayEquals(
                original.getSerializedTableSchema(), deserialized.getSerializedTableSchema());
    }
}
