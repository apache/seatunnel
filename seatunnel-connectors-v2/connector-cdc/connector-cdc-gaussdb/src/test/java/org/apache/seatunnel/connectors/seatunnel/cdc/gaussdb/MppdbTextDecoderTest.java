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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Tests serial, JSON, text, and framed mppdb decoding behavior. */
class MppdbTextDecoderTest {

    /** Decoder under test. */
    private final MppdbTextDecoder decoder = new MppdbTextDecoder();

    /** Verifies transaction ids in serial BEGIN and COMMIT records. */
    @Test
    void testDecodeSerialTransactionRecords() {
        MppdbWalChange begin = decoder.decodeRecord(10L, 0L, "BEGIN 78108");
        MppdbWalChange commit = decoder.decodeRecord(20L, 0L, "COMMIT XID: 78108 CSN: 2358");

        Assertions.assertEquals(MppdbWalChange.Type.BEGIN, begin.getType());
        Assertions.assertEquals(78108L, begin.getTransactionId());
        Assertions.assertEquals(MppdbWalChange.Type.COMMIT, commit.getType());
        Assertions.assertEquals(78108L, commit.getTransactionId());
    }

    /** Verifies INSERT columns, quoted SQL strings, and SQL NULL values. */
    @Test
    void testDecodeJsonInsertAndSqlLiterals() {
        String json =
                "{\"table_name\":\"public.customers\","
                        + "\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"id\",\"name\",\"note\"],"
                        + "\"columns_type\":[\"integer\",\"character varying\",\"text\"],"
                        + "\"columns_val\":[\"1\",\"'O''Brien'\",\"null\"]}";

        MppdbWalChange change = decoder.decodeRecord(100L, 42L, json);

        Assertions.assertEquals(MppdbWalChange.Type.INSERT, change.getType());
        Assertions.assertEquals("public", change.getSchema());
        Assertions.assertEquals("customers", change.getTable());
        Assertions.assertEquals(3, change.getNewColumns().size());
        Assertions.assertEquals("O'Brien", change.getNewColumns().get(1).getValue());
        Assertions.assertTrue(change.getNewColumns().get(2).isNullValue());
    }

    /** Verifies UPDATE and DELETE tuple selection. */
    @Test
    void testDecodeJsonUpdateAndDelete() {
        String update =
                "{\"table_name\":\"inventory.orders\",\"op_type\":\"UPDATE\","
                        + "\"columns_name\":[\"id\",\"status\"],"
                        + "\"columns_type\":[\"integer\",\"text\"],"
                        + "\"columns_val\":[\"7\",\"'paid'\"],"
                        + "\"old_keys_name\":[\"id\"],\"old_keys_type\":[\"integer\"],"
                        + "\"old_keys_val\":[\"7\"]}";
        String delete =
                "{\"table_name\":\"inventory.orders\",\"op_type\":\"DELETE\","
                        + "\"old_keys_name\":[\"id\"],\"old_keys_type\":[\"integer\"],"
                        + "\"old_keys_val\":[\"7\"]}";

        MppdbWalChange updateChange = decoder.decodeRecord(101L, 43L, update);
        MppdbWalChange deleteChange = decoder.decodeRecord(102L, 44L, delete);

        Assertions.assertEquals(MppdbWalChange.Type.UPDATE, updateChange.getType());
        Assertions.assertEquals("paid", updateChange.getNewColumns().get(1).getValue());
        Assertions.assertEquals("7", updateChange.getOldColumns().get(0).getValue());
        Assertions.assertEquals(MppdbWalChange.Type.DELETE, deleteChange.getType());
        Assertions.assertTrue(deleteChange.getNewColumns().isEmpty());
        Assertions.assertEquals("id", deleteChange.getOldColumns().get(0).getName());
    }

    /** Verifies the documented parallel text UPDATE representation. */
    @Test
    void testDecodeDocumentedTextUpdate() {
        String text =
                "table public t1 UPDATE: old-key: a[integer]:1 b[integer]:2 "
                        + "c[text]:'old value' new-tuple: a[integer]:1 b[integer]:5 "
                        + "c[text]:'new value'";

        MppdbWalChange change = decoder.decodeRecord(200L, 45L, text);

        Assertions.assertEquals(MppdbWalChange.Type.UPDATE, change.getType());
        Assertions.assertEquals("public", change.getSchema());
        Assertions.assertEquals("t1", change.getTable());
        Assertions.assertEquals("old value", change.getOldColumns().get(2).getValue());
        Assertions.assertEquals("new value", change.getNewColumns().get(2).getValue());
    }

    /** Verifies length-framed parallel textual batches. */
    @Test
    void testDecodeFramedJsonBatch() {
        String begin = "BEGIN CSN: 2358 first_lsn: 0/CFE6220";
        String insert =
                "{\"table_name\":\"public.t1\",\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"a\"],\"columns_type\":[\"integer\"],"
                        + "\"columns_val\":[\"3\"],\"old_keys_name\":[],"
                        + "\"old_keys_type\":[],\"old_keys_val\":[]}";
        byte[] payload = framedTextBatch(frame(1000L, begin), frame(1001L, insert));

        List<MppdbWalChange> changes = decoder.decodeBatch(payload);

        Assertions.assertEquals(2, changes.size());
        Assertions.assertEquals(1000L, changes.get(0).getLsn());
        Assertions.assertEquals(MppdbWalChange.Type.BEGIN, changes.get(0).getType());
        Assertions.assertEquals(1001L, changes.get(1).getLsn());
        Assertions.assertEquals(MppdbWalChange.Type.INSERT, changes.get(1).getType());
    }

    /** Verifies malformed arrays and truncated frames fail fast. */
    @Test
    void testRejectMisalignedJsonColumnsAndTruncatedFrame() {
        String invalidJson =
                "{\"table_name\":\"public.t1\",\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"a\"],\"columns_type\":[],"
                        + "\"columns_val\":[\"1\"],\"old_keys_name\":[],"
                        + "\"old_keys_type\":[],\"old_keys_val\":[]}";
        ByteBuffer truncated = ByteBuffer.allocate(8);
        truncated.putInt(20).putInt(1);

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> decoder.decodeRecord(1L, 1L, invalidJson));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> decoder.decodeBatch(truncated.array()));
    }

    /** Encodes one length-prefixed textual frame for tests. */
    private byte[] frame(long lsn, String record) {
        byte[] data = record.getBytes(StandardCharsets.UTF_8);
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + data.length);
        frame.putInt(Long.BYTES + data.length).putLong(lsn).put(data);
        return frame.array();
    }

    /** Joins frames and appends the zero-length batch terminator. */
    private byte[] framedTextBatch(byte[]... frames) {
        int size = Integer.BYTES;
        for (byte[] frame : frames) {
            size += frame.length;
        }
        ByteBuffer batch = ByteBuffer.allocate(size);
        for (byte[] frame : frames) {
            batch.put(frame);
        }
        batch.putInt(0);
        return batch.array();
    }
}
