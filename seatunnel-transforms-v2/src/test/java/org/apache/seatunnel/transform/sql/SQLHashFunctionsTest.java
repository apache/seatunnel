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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.shade.com.google.common.hash.Hashing;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/** Tests for hash functions like MURMUR64 */
public class SQLHashFunctionsTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    private static Long murmur64Direct(String input) {
        if (input == null) {
            return null;
        }
        return Hashing.murmur3_128().hashString(input, StandardCharsets.UTF_8).asLong();
    }

    @Test
    public void testMurmur64WithNormalString() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql("select MURMUR64(text) as hash from dual", rowType, "hello world");

        Assertions.assertInstanceOf(Long.class, outRow.getField(0));
        Assertions.assertEquals(murmur64Direct("hello world"), outRow.getField(0));
    }

    @Test
    public void testMurmur64WithEmptyString() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow = runSql("select MURMUR64(text) as hash from dual", rowType, "");

        Assertions.assertInstanceOf(Long.class, outRow.getField(0));
        Assertions.assertEquals(murmur64Direct(""), outRow.getField(0));
    }

    @Test
    public void testMurmur64WithNull() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql("select MURMUR64(text) as hash from dual", rowType, (Object) null);

        Assertions.assertNull(outRow.getField(0));
    }

    @Test
    public void testMurmur64Consistency() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        // Same input should always produce same hash
        SeaTunnelRow outRow1 =
                runSql("select MURMUR64(text) as hash from dual", rowType, "test123");
        SeaTunnelRow outRow2 =
                runSql("select MURMUR64(text) as hash from dual", rowType, "test123");

        Assertions.assertInstanceOf(Long.class, outRow1.getField(0));
        Assertions.assertEquals(outRow1.getField(0), outRow2.getField(0));
        Assertions.assertEquals(murmur64Direct("test123"), outRow1.getField(0));
    }
}
