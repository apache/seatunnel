/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more
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
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.calcite.engine.CalciteSQLEngine;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Engine-parity tests for the ENCODE/DECODE charset conversion functions.
 *
 * <p>Every case runs through both SQL engines — the default Zeta SQL transform and the Calcite SQL
 * transform — with the same assertions: the six allowed charsets, case-insensitive charset names,
 * blank and unsupported charset errors, NULL propagation, wrong argument count/type errors, invalid
 * byte/character replacement, and output types.
 */
public class SQLEncodeDecodeFunctionsTest {

    private static final String NAME = "café中国";

    // ---------- harness: Zeta SQL transform ----------

    private SQLTransform zetaTransform(String query, SeaTunnelRowType rowType) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        return new SQLTransform(config, table);
    }

    private SeaTunnelRow zetaRow(String query, SeaTunnelRowType rowType, Object... values) {
        List<SeaTunnelRow> out =
                zetaTransform(query, rowType).transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    private SqlType zetaOutputSqlType(String query, SeaTunnelRowType rowType) {
        return zetaTransform(query, rowType)
                .getProducedCatalogTable()
                .getSeaTunnelRowType()
                .getFieldType(0)
                .getSqlType();
    }

    // ---------- harness: Calcite SQL transform ----------

    private CalciteSQLEngine calciteEngine(String sql, SeaTunnelRowType rowType) {
        CalciteSQLEngine engine = new CalciteSQLEngine(sql, "test", rowType);
        engine.init();
        return engine;
    }

    private Object calciteSingle(String sql, SeaTunnelRowType rowType, Object... values) {
        CalciteSQLEngine engine = calciteEngine(sql, rowType);
        try {
            return engine.execute(new SeaTunnelRow(values)).get(0).getField(0);
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {
                // closing a used engine must not mask the assertion
            }
        }
    }

    // ---------- shared helpers ----------

    private SeaTunnelRowType nameRowType() {
        return new SeaTunnelRowType(
                new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }

    private SeaTunnelRowType idNameRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name"},
                new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
    }

    private SeaTunnelRowType payloadRowType() {
        return new SeaTunnelRowType(
                new String[] {"payload"},
                new SeaTunnelDataType[] {PrimitiveByteArrayType.INSTANCE});
    }

    private static byte[] hex(String hexString) {
        byte[] bytes = new byte[hexString.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hexString.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    // ---------- 1. ENCODE with the six allowed charsets ----------

    @Test
    public void testEncodeSixCharsets() {
        String query =
                "select ENCODE(name, 'US-ASCII') as c1,"
                        + " ENCODE(name, 'ISO-8859-1') as c2,"
                        + " ENCODE(name, 'UTF-8') as c3,"
                        + " ENCODE(name, 'UTF-16BE') as c4,"
                        + " ENCODE(name, 'UTF-16LE') as c5,"
                        + " ENCODE(name, 'UTF-16') as c6"
                        + " from dual";
        SeaTunnelRow zeta = zetaRow(query, nameRowType(), NAME);

        String sql =
                "SELECT ENCODE(name, 'US-ASCII') AS c1,"
                        + " ENCODE(name, 'ISO-8859-1') AS c2,"
                        + " ENCODE(name, 'UTF-8') AS c3,"
                        + " ENCODE(name, 'UTF-16BE') AS c4,"
                        + " ENCODE(name, 'UTF-16LE') AS c5,"
                        + " ENCODE(name, 'UTF-16') AS c6"
                        + " FROM test";
        CalciteSQLEngine engine = calciteEngine(sql, nameRowType());
        SeaTunnelRow calcite = engine.execute(new SeaTunnelRow(new Object[] {NAME})).get(0);
        try {
            for (int i = 0; i < 6; i++) {
                Assertions.assertArrayEquals(ENCODED_NAME_BYTES[i], (byte[]) zeta.getField(i));
                Assertions.assertArrayEquals(ENCODED_NAME_BYTES[i], (byte[]) calcite.getField(i));
            }
        } finally {
            try {
                engine.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static final byte[][] ENCODED_NAME_BYTES = {
        hex("6361663F3F3F"), // US-ASCII: unmappable characters become '?'
        hex("636166E93F3F"), // ISO-8859-1: e-acute kept, unmappable characters become '?'
        hex("636166C3A9E4B8ADE59BBD"), // UTF-8
        hex("00630061006600E94E2D56FD"), // UTF-16BE
        hex("630061006600E9002D4EFD56"), // UTF-16LE
        hex("FEFF00630061006600E94E2D56FD") // UTF-16 with BOM
    };

    // ---------- 2. DECODE consumes a binary column ----------

    @Test
    public void testDecodeBinaryColumn() {
        byte[] payload = hex("0053007000610072006B002000530051004C");
        SeaTunnelRow zeta =
                zetaRow(
                        "select DECODE(payload, 'UTF-16BE') as v from dual",
                        payloadRowType(),
                        payload);
        Object calcite =
                calciteSingle(
                        "SELECT DECODE(payload, 'UTF-16BE') AS v FROM test",
                        payloadRowType(),
                        payload);
        Assertions.assertEquals("Spark SQL", zeta.getField(0));
        Assertions.assertEquals("Spark SQL", calcite);
    }

    // ---------- 3. charset names are case insensitive ----------

    @Test
    public void testCharsetNameCaseInsensitive() {
        byte[] expected = hex("00630061006600E94E2D56FD");
        SeaTunnelRow zeta =
                zetaRow("select ENCODE(name, 'utf-16be') as v from dual", nameRowType(), NAME);
        Object calcite =
                calciteSingle(
                        "SELECT ENCODE(name, 'utf-16be') AS v FROM test", nameRowType(), NAME);
        Assertions.assertArrayEquals(expected, (byte[]) zeta.getField(0));
        Assertions.assertArrayEquals(expected, (byte[]) calcite);
    }

    // ---------- 4. blank charset is an error ----------

    @Test
    public void testBlankCharsetRejected() {
        for (String blank : new String[] {"''", "'  '"}) {
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            zetaRow(
                                    "select ENCODE(name, " + blank + ") as v from dual",
                                    nameRowType(),
                                    NAME));
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            calciteSingle(
                                    "SELECT ENCODE(name, " + blank + ") AS v FROM test",
                                    nameRowType(),
                                    NAME));
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            zetaRow(
                                    "select DECODE(payload, " + blank + ") as v from dual",
                                    payloadRowType(),
                                    hex("00")));
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            calciteSingle(
                                    "SELECT DECODE(payload, " + blank + ") AS v FROM test",
                                    payloadRowType(),
                                    hex("00")));
        }
    }

    // ---------- 5. unsupported charset is an error, even when the JVM knows it ----------

    @Test
    public void testUnsupportedCharsetRejected() {
        // 'GBK' is a real JVM charset and 'utf8' is a JVM alias: the six-name allowlist
        // rejects both, so behavior does not depend on the JVM's charset registry
        for (String unsupported : new String[] {"'GBK'", "'utf-32'", "'utf8'"}) {
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            zetaRow(
                                    "select ENCODE(name, " + unsupported + ") as v from dual",
                                    nameRowType(),
                                    NAME));
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            calciteSingle(
                                    "SELECT ENCODE(name, " + unsupported + ") AS v FROM test",
                                    nameRowType(),
                                    NAME));
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            zetaRow(
                                    "select DECODE(payload, " + unsupported + ") as v from dual",
                                    payloadRowType(),
                                    hex("00")));
            Assertions.assertThrows(
                    RuntimeException.class,
                    () ->
                            calciteSingle(
                                    "SELECT DECODE(payload, " + unsupported + ") AS v FROM test",
                                    payloadRowType(),
                                    hex("00")));
        }
    }

    // ---------- 6. NULL propagation ----------

    @Test
    public void testNullValuePropagation() {
        // value is NULL
        Assertions.assertNull(
                zetaRow("select ENCODE(NULL, 'UTF-8') as v from dual", nameRowType(), NAME)
                        .getField(0));
        Assertions.assertNull(
                calciteSingle(
                        "SELECT ENCODE(CAST(NULL AS VARCHAR), 'UTF-8') AS v FROM test",
                        nameRowType(),
                        NAME));
        Assertions.assertNull(
                zetaRow("select DECODE(NULL, 'UTF-8') as v from dual", nameRowType(), NAME)
                        .getField(0));
        Assertions.assertNull(
                calciteSingle(
                        "SELECT DECODE(CAST(NULL AS VARBINARY), 'UTF-8') AS v FROM test",
                        payloadRowType(),
                        hex("00")));

        // charset is NULL
        Assertions.assertNull(
                zetaRow("select ENCODE(name, NULL) as v from dual", nameRowType(), NAME)
                        .getField(0));
        Assertions.assertNull(
                calciteSingle(
                        "SELECT ENCODE(name, CAST(NULL AS VARCHAR)) AS v FROM test",
                        nameRowType(),
                        NAME));
        Assertions.assertNull(
                zetaRow("select DECODE(payload, NULL) as v from dual", payloadRowType(), hex("00"))
                        .getField(0));
        Assertions.assertNull(
                calciteSingle(
                        "SELECT DECODE(payload, CAST(NULL AS VARCHAR)) AS v FROM test",
                        payloadRowType(),
                        hex("00")));

        // a NULL column value also propagates
        Assertions.assertNull(
                zetaRow("select ENCODE(name, 'UTF-8') as v from dual", nameRowType(), (Object) null)
                        .getField(0));
        Assertions.assertNull(
                calciteSingle(
                        "SELECT ENCODE(name, 'UTF-8') AS v FROM test",
                        nameRowType(),
                        new Object[] {null}));
        Assertions.assertNull(
                zetaRow(
                                "select DECODE(payload, 'UTF-8') as v from dual",
                                payloadRowType(),
                                (Object) null)
                        .getField(0));
        Assertions.assertNull(
                calciteSingle(
                        "SELECT DECODE(payload, 'UTF-8') AS v FROM test",
                        payloadRowType(),
                        new Object[] {null}));

        // NULL wins over charset validation: a NULL value with an unsupported charset name
        // still returns NULL instead of raising the charset error
        Assertions.assertNull(
                zetaRow(
                                "select DECODE(ENCODE(NULL, 'GBK'), 'GBK') as v from dual",
                                nameRowType(),
                                NAME)
                        .getField(0));
        Assertions.assertNull(
                calciteSingle(
                        "SELECT DECODE(ENCODE(CAST(NULL AS VARCHAR), 'GBK'), 'GBK') AS v FROM test",
                        nameRowType(),
                        NAME));
    }

    // ---------- 7. wrong argument count is an error ----------

    @Test
    public void testWrongArgumentCountRejected() {
        Assertions.assertThrows(
                RuntimeException.class,
                () -> zetaRow("select ENCODE(name) as v from dual", nameRowType(), NAME));
        Assertions.assertThrows(
                RuntimeException.class,
                () -> calciteSingle("SELECT ENCODE(name) FROM test", nameRowType(), NAME));
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        zetaRow(
                                "select DECODE(payload, 'UTF-8', 'extra') as v from dual",
                                payloadRowType(),
                                hex("00")));
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        calciteSingle(
                                "SELECT DECODE(payload, 'UTF-8', 'extra') FROM test",
                                payloadRowType(),
                                hex("00")));
    }

    // ---------- 8. wrong argument type is an error ----------

    @Test
    public void testWrongArgumentTypeRejected() {
        // DECODE requires a binary value: an int column is rejected by both engines (the Calcite
        // validator has no implicit numeric-to-VARBINARY cast)
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        zetaRow(
                                "select DECODE(id, 'UTF-8') as v from dual",
                                idNameRowType(),
                                1,
                                NAME));
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        calciteSingle(
                                "SELECT DECODE(id, 'UTF-8') AS v FROM test",
                                idNameRowType(),
                                1,
                                NAME));
        // ENCODE requires a string value: a binary column is rejected by the Zeta engine
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        zetaRow(
                                "select ENCODE(payload, 'UTF-8') as v from dual",
                                payloadRowType(),
                                hex("00")));
        // DECODE requires a binary value: a string column is rejected by the Zeta engine
        Assertions.assertThrows(
                RuntimeException.class,
                () -> zetaRow("select DECODE(name, 'UTF-8') as v from dual", nameRowType(), NAME));
    }

    // ---------- 9. invalid bytes/characters are replaced, not errors ----------

    @Test
    public void testInvalidSequenceReplaced() {
        // malformed input bytes decode to the JVM replacement character
        SeaTunnelRow zeta =
                zetaRow(
                        "select DECODE(payload, 'US-ASCII') as v from dual",
                        payloadRowType(),
                        hex("FF"));
        Object calcite =
                calciteSingle(
                        "SELECT DECODE(payload, 'US-ASCII') AS v FROM test",
                        payloadRowType(),
                        hex("FF"));
        Assertions.assertEquals("\uFFFD", zeta.getField(0));
        Assertions.assertEquals("\uFFFD", calcite);

        // odd-length UTF-16BE: trailing half character is replaced
        zeta =
                zetaRow(
                        "select DECODE(payload, 'UTF-16BE') as v from dual",
                        payloadRowType(),
                        hex("005300"));
        calcite =
                calciteSingle(
                        "SELECT DECODE(payload, 'UTF-16BE') AS v FROM test",
                        payloadRowType(),
                        hex("005300"));
        Assertions.assertEquals("S\uFFFD", zeta.getField(0));
        Assertions.assertEquals("S\uFFFD", calcite);

        // unmappable character encodes to '?'
        zeta = zetaRow("select ENCODE(name, 'US-ASCII') as v from dual", nameRowType(), "中");
        calcite =
                calciteSingle("SELECT ENCODE(name, 'US-ASCII') AS v FROM test", nameRowType(), "中");
        Assertions.assertArrayEquals(hex("3F"), (byte[]) zeta.getField(0));
        Assertions.assertArrayEquals(hex("3F"), (byte[]) calcite);
    }

    // ---------- 10. output types ----------

    @Test
    public void testOutputTypes() {
        Assertions.assertEquals(
                SqlType.BYTES,
                zetaOutputSqlType("select ENCODE(name, 'UTF-8') as v from dual", nameRowType()));
        Assertions.assertEquals(
                SqlType.STRING,
                zetaOutputSqlType(
                        "select DECODE(payload, 'UTF-8') as v from dual", payloadRowType()));

        CalciteSQLEngine encodeEngine =
                calciteEngine("SELECT ENCODE(name, 'UTF-8') AS v FROM test", nameRowType());
        try {
            Assertions.assertEquals(
                    SqlType.BYTES, encodeEngine.getOutputRowType().getFieldType(0).getSqlType());
        } finally {
            try {
                encodeEngine.close();
            } catch (Exception ignored) {
            }
        }
        CalciteSQLEngine decodeEngine =
                calciteEngine("SELECT DECODE(payload, 'UTF-8') AS v FROM test", payloadRowType());
        try {
            Assertions.assertEquals(
                    SqlType.STRING, decodeEngine.getOutputRowType().getFieldType(0).getSqlType());
        } finally {
            try {
                decodeEngine.close();
            } catch (Exception ignored) {
            }
        }
    }
}
