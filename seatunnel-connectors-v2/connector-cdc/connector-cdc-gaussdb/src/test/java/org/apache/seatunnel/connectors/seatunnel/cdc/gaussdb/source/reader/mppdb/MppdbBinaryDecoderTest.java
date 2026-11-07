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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.source.reader.mppdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Tests strict decoding of framed mppdb binary records. */
class MppdbBinaryDecoderTest {

    /** Decoder under test. */
    private final MppdbBinaryDecoder decoder = new MppdbBinaryDecoder();

    /** Verifies INSERT values and the binary SQL NULL marker. */
    @Test
    void testDecodeInsertWithNullColumn() {
        byte[] record =
                record(
                        100L,
                        'I',
                        buffer -> {
                            putString16(buffer, "public");
                            putString16(buffer, "customers");
                            buffer.put((byte) 'N').putShort((short) 2);
                            putColumn(buffer, "id", 23, "1");
                            putNullColumn(buffer, "name", 25);
                        });

        MppdbWalChange change = decoder.decode(batch(record)).get(0);

        Assertions.assertEquals(MppdbWalChange.Type.INSERT, change.getType());
        Assertions.assertEquals("public", change.getSchema());
        Assertions.assertEquals("customers", change.getTable());
        Assertions.assertEquals(23, change.getNewColumns().get(0).getTypeOid());
        Assertions.assertTrue(change.getNewColumns().get(1).isNullValue());
    }

    /** Verifies UPDATE records carrying both new and replica identity tuples. */
    @Test
    void testDecodeUpdateWithNewAndOldTuples() {
        byte[] record =
                record(
                        200L,
                        'U',
                        buffer -> {
                            putString16(buffer, "inventory");
                            putString16(buffer, "orders");
                            buffer.put((byte) 'N').putShort((short) 2);
                            putColumn(buffer, "id", 23, "7");
                            putColumn(buffer, "status", 25, "paid");
                            buffer.put((byte) 'O').putShort((short) 1);
                            putColumn(buffer, "id", 23, "7");
                        });

        MppdbWalChange change = decoder.decode(batch(record)).get(0);

        Assertions.assertEquals(MppdbWalChange.Type.UPDATE, change.getType());
        Assertions.assertEquals("paid", change.getNewColumns().get(1).getValue());
        Assertions.assertEquals("7", change.getOldColumns().get(0).getValue());
    }

    /** Verifies batch framing and COMMIT transaction ids. */
    @Test
    void testDecodeMultipleRecordsAndCommitXid() {
        byte[] begin = record(300L, 'B', buffer -> buffer.putLong(99L).putLong(300L));
        byte[] commit = record(301L, 'C', buffer -> buffer.put((byte) 'X').putLong(78108L));

        List<MppdbWalChange> changes = decoder.decode(batch(begin, commit));

        Assertions.assertEquals(2, changes.size());
        Assertions.assertEquals(MppdbWalChange.Type.BEGIN, changes.get(0).getType());
        Assertions.assertEquals(MppdbWalChange.Type.COMMIT, changes.get(1).getType());
        Assertions.assertEquals(78108L, changes.get(1).getTransactionId());
    }

    /** Verifies truncated and unknown record kinds fail fast. */
    @Test
    void testRejectTruncatedAndUnknownRecords() {
        ByteBuffer truncated = ByteBuffer.allocate(8);
        truncated.putInt(20).putInt(1);
        byte[] unknown = record(400L, 'Z', buffer -> {});

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> decoder.decode(truncated.array()));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> decoder.decode(batch(unknown)));
    }

    /** Encodes one bounded binary record body for tests. */
    private byte[] record(long lsn, char kind, RecordWriter writer) {
        ByteBuffer body = ByteBuffer.allocate(1024);
        body.putLong(lsn).put((byte) kind);
        writer.write(body);
        body.flip();
        byte[] record = new byte[body.remaining()];
        body.get(record);
        return record;
    }

    /** Frames records using the mppdb P/F separators. */
    private byte[] batch(byte[]... records) {
        int size = 0;
        for (byte[] record : records) {
            size += Integer.BYTES + record.length + Byte.BYTES;
        }
        ByteBuffer batch = ByteBuffer.allocate(size);
        for (int index = 0; index < records.length; index++) {
            batch.putInt(records[index].length).put(records[index]);
            batch.put((byte) (index == records.length - 1 ? 'F' : 'P'));
        }
        return batch.array();
    }

    /** Writes one non-null binary tuple column. */
    private void putColumn(ByteBuffer buffer, String name, int typeOid, String value) {
        putString16(buffer, name);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(typeOid).putInt(bytes.length).put(bytes);
    }

    /** Writes one binary tuple column with the SQL NULL length marker. */
    private void putNullColumn(ByteBuffer buffer, String name, int typeOid) {
        putString16(buffer, name);
        buffer.putInt(typeOid).putInt(-1);
    }

    /** Writes a uint16-length-prefixed UTF-8 string. */
    private void putString16(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) bytes.length).put(bytes);
    }

    /** Writes a record-specific body into a test buffer. */
    @FunctionalInterface
    private interface RecordWriter {
        /** Appends record fields to the supplied buffer. */
        void write(ByteBuffer buffer);
    }
}
