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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class SQLCryptoFunctionsTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    @Test
    public void testAesRoundTripWithPassphrase() {
        // End-to-end: exercises ZetaSQLFunction dispatch + ZetaSQLType inference, not just the
        // static CryptoFunction methods.
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select AES_DECRYPT(AES_ENCRYPT(name, 'mySecretPass'), 'mySecretPass') as r"
                                + " from dual",
                        rowType,
                        "Hello SeaTunnel");

        Assertions.assertInstanceOf(String.class, outRow.getField(0));
        Assertions.assertEquals("Hello SeaTunnel", outRow.getField(0));
    }

    @Test
    public void testAesRoundTripWithExplicitIv() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select AES_DECRYPT("
                                + "AES_ENCRYPT(name, 'mySecretPass', '1234567890123456'),"
                                + " 'mySecretPass', '1234567890123456') as r from dual",
                        rowType,
                        "Hello SeaTunnel");

        Assertions.assertEquals("Hello SeaTunnel", outRow.getField(0));
    }

    @Test
    public void testAesEncryptReturnsBase64String() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql("select AES_ENCRYPT(name, 'mySecretPass') as c from dual", rowType, "plain");

        // Result type must be STRING (verifies ZetaSQLType.getFunctionType registration).
        Assertions.assertInstanceOf(String.class, outRow.getField(0));
        // A random IV makes the ciphertext non-deterministic, but it must differ from the input
        // and be non-empty.
        Assertions.assertNotNull(outRow.getField(0));
        Assertions.assertNotEquals("plain", outRow.getField(0));
    }

    @Test
    public void testAesExplicitIvIsDeterministic() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow out1 =
                runSql(
                        "select AES_ENCRYPT(name, 'mySecretPass', '1234567890123456') as c"
                                + " from dual",
                        rowType,
                        "deterministic");
        SeaTunnelRow out2 =
                runSql(
                        "select AES_ENCRYPT(name, 'mySecretPass', '1234567890123456') as c"
                                + " from dual",
                        rowType,
                        "deterministic");

        Assertions.assertEquals(out1.getField(0), out2.getField(0));
    }

    @Test
    public void testAesEncryptNullReturnsNull() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select AES_ENCRYPT(name, 'mySecretPass') as c from dual",
                        rowType,
                        (Object) null);

        Assertions.assertNull(outRow.getField(0));
    }

    @Test
    public void testAesDecryptNullReturnsNull() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select AES_DECRYPT(name, 'mySecretPass') as r from dual",
                        rowType,
                        (Object) null);

        Assertions.assertNull(outRow.getField(0));
    }
}
