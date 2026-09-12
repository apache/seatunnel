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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Strict decoder for the framed binary output of GaussDB {@code mppdb_decoding}. */
final class MppdbBinaryDecoder {

    /** Unsigned uint32 marker used by mppdb for SQL NULL. */
    private static final long NULL_VALUE_LENGTH = 0xFFFFFFFFL;

    /** Minimum bytes in a record after the uint32 frame length: LSN plus record kind. */
    private static final int MIN_RECORD_SIZE = Long.BYTES + Byte.BYTES;

    /** Decodes one server batch and rejects truncated or misaligned protocol frames. */
    List<MppdbWalChange> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return Collections.emptyList();
        }

        ByteBuffer batch = ByteBuffer.wrap(payload);
        List<MppdbWalChange> changes = new ArrayList<>();
        while (batch.hasRemaining()) {
            requireRemaining(batch, Integer.BYTES, "record length");
            long unsignedSize = Integer.toUnsignedLong(batch.getInt());
            if (unsignedSize == 0) {
                if (batch.hasRemaining()) {
                    throw malformed("bytes found after the binary batch terminator");
                }
                break;
            }
            if (unsignedSize > Integer.MAX_VALUE || unsignedSize < MIN_RECORD_SIZE) {
                throw malformed("invalid record length " + unsignedSize);
            }
            int recordSize = (int) unsignedSize;
            requireRemaining(batch, recordSize, "record body");

            ByteBuffer record = batch.slice();
            record.limit(recordSize);
            batch.position(batch.position() + recordSize);
            long lsn = record.getLong();
            byte kind = record.get();
            changes.add(decodeRecord(record, lsn, kind));
            if (record.hasRemaining()) {
                throw malformed(
                        "record "
                                + (char) kind
                                + " contains "
                                + record.remaining()
                                + " trailing bytes");
            }

            // Parallel decoding places P/F outside the length-delimited record.
            if (batch.hasRemaining()
                    && (batch.get(batch.position()) == 'P' || batch.get(batch.position()) == 'F')) {
                byte separator = batch.get();
                if (separator == 'F') {
                    if (batch.remaining() == Integer.BYTES && batch.getInt() == 0) {
                        break;
                    }
                    if (batch.hasRemaining()) {
                        throw malformed("bytes found after the final record separator");
                    }
                    break;
                }
            }
        }
        return changes;
    }

    /** Decodes a single bounded record body. */
    private MppdbWalChange decodeRecord(ByteBuffer record, long lsn, byte kind) {
        switch (kind) {
            case 'B':
                return decodeBegin(record, lsn);
            case 'C':
                return decodeCommit(record, lsn);
            case 'I':
                return decodeInsert(record, lsn);
            case 'U':
                return decodeUpdate(record, lsn);
            case 'D':
                return decodeDelete(record, lsn);
            default:
                throw malformed("unsupported record kind " + (char) kind);
        }
    }

    /** Decodes a BEGIN record and consumes its optional timestamp and user fields. */
    private MppdbWalChange decodeBegin(ByteBuffer record, long lsn) {
        requireRemaining(record, Long.BYTES * 2, "BEGIN body");
        record.getLong(); // CSN is ordering metadata and is not exposed by Debezium records.
        record.getLong(); // first_lsn is redundant with the frame LSN for SeaTunnel offsets.
        while (record.hasRemaining()) {
            byte marker = record.get();
            if (marker != 'T' && marker != 'N') {
                throw malformed("unsupported BEGIN optional field " + (char) marker);
            }
            readInt32String(record, "BEGIN optional field");
        }
        return new MppdbWalChange(lsn, 0, MppdbWalChange.Type.BEGIN, null, null, null, null);
    }

    /** Decodes a COMMIT record and its optional transaction metadata. */
    private MppdbWalChange decodeCommit(ByteBuffer record, long lsn) {
        long transactionId = 0;
        while (record.hasRemaining()) {
            byte marker = record.get();
            if (marker == 'X') {
                requireRemaining(record, Long.BYTES, "COMMIT transaction id");
                transactionId = record.getLong();
            } else if (marker == 'T') {
                readInt32String(record, "COMMIT timestamp");
            } else {
                throw malformed("unsupported COMMIT optional field " + (char) marker);
            }
        }
        return new MppdbWalChange(
                lsn, transactionId, MppdbWalChange.Type.COMMIT, null, null, null, null);
    }

    /** Decodes a complete inserted row. */
    private MppdbWalChange decodeInsert(ByteBuffer record, long lsn) {
        String schema = readInt16String(record, "schema name");
        String table = readInt16String(record, "table name");
        consumeTupleMarker(record, (byte) 'N');
        List<MppdbWalChange.ColumnValue> columns = decodeColumns(record);
        return new MppdbWalChange(lsn, 0, MppdbWalChange.Type.INSERT, schema, table, null, columns);
    }

    /** Decodes new row values and optional replica identity values for an update. */
    private MppdbWalChange decodeUpdate(ByteBuffer record, long lsn) {
        String schema = readInt16String(record, "schema name");
        String table = readInt16String(record, "table name");
        List<MppdbWalChange.ColumnValue> oldColumns = Collections.emptyList();
        List<MppdbWalChange.ColumnValue> newColumns = Collections.emptyList();
        while (record.hasRemaining()) {
            byte marker = record.get();
            if (marker == 'N') {
                newColumns = decodeColumns(record);
            } else if (marker == 'O') {
                oldColumns = decodeColumns(record);
            } else {
                throw malformed("expected UPDATE tuple marker but found " + (char) marker);
            }
        }
        if (newColumns.isEmpty()) {
            throw malformed("UPDATE record does not contain a new tuple");
        }
        return new MppdbWalChange(
                lsn, 0, MppdbWalChange.Type.UPDATE, schema, table, oldColumns, newColumns);
    }

    /** Decodes replica identity values for a deleted row. */
    private MppdbWalChange decodeDelete(ByteBuffer record, long lsn) {
        String schema = readInt16String(record, "schema name");
        String table = readInt16String(record, "table name");
        if (!record.hasRemaining()) {
            return new MppdbWalChange(
                    lsn,
                    0,
                    MppdbWalChange.Type.DELETE,
                    schema,
                    table,
                    Collections.emptyList(),
                    null);
        }
        byte marker = record.get();
        if (marker != 'O' && marker != 'N') {
            throw malformed("expected DELETE tuple marker but found " + (char) marker);
        }
        return new MppdbWalChange(
                lsn, 0, MppdbWalChange.Type.DELETE, schema, table, decodeColumns(record), null);
    }

    /** Decodes one mppdb tuple using length-prefixed names and text values. */
    private List<MppdbWalChange.ColumnValue> decodeColumns(ByteBuffer record) {
        requireRemaining(record, Short.BYTES, "column count");
        int columnCount = Short.toUnsignedInt(record.getShort());
        List<MppdbWalChange.ColumnValue> columns = new ArrayList<>(columnCount);
        for (int index = 0; index < columnCount; index++) {
            String name = readInt16String(record, "column name");
            requireRemaining(record, Integer.BYTES * 2, "column type and value length");
            int typeOid = record.getInt();
            long valueLength = Integer.toUnsignedLong(record.getInt());
            if (valueLength == NULL_VALUE_LENGTH) {
                columns.add(new MppdbWalChange.ColumnValue(name, typeOid, null, null, true));
                continue;
            }
            if (valueLength > Integer.MAX_VALUE) {
                throw malformed("column value is too large: " + valueLength);
            }
            requireRemaining(record, (int) valueLength, "column value");
            byte[] value = new byte[(int) valueLength];
            record.get(value);
            columns.add(
                    new MppdbWalChange.ColumnValue(
                            name, typeOid, null, new String(value, StandardCharsets.UTF_8), false));
        }
        return columns;
    }

    /** Consumes and validates the required tuple marker. */
    private void consumeTupleMarker(ByteBuffer record, byte expected) {
        requireRemaining(record, Byte.BYTES, "tuple marker");
        byte actual = record.get();
        if (actual != expected) {
            throw malformed(
                    "expected tuple marker " + (char) expected + " but found " + (char) actual);
        }
    }

    /** Reads a uint16-length-prefixed UTF-8 string. */
    private String readInt16String(ByteBuffer record, String field) {
        requireRemaining(record, Short.BYTES, field + " length");
        int length = Short.toUnsignedInt(record.getShort());
        requireRemaining(record, length, field);
        byte[] bytes = new byte[length];
        record.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Reads a uint32-length-prefixed UTF-8 string. */
    private String readInt32String(ByteBuffer record, String field) {
        requireRemaining(record, Integer.BYTES, field + " length");
        long unsignedLength = Integer.toUnsignedLong(record.getInt());
        if (unsignedLength > Integer.MAX_VALUE) {
            throw malformed(field + " is too large: " + unsignedLength);
        }
        int length = (int) unsignedLength;
        requireRemaining(record, length, field);
        byte[] bytes = new byte[length];
        record.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Verifies that a bounded frame has enough bytes before consuming a protocol field. */
    private void requireRemaining(ByteBuffer buffer, int required, String field) {
        if (required < 0 || buffer.remaining() < required) {
            throw malformed(
                    "truncated "
                            + field
                            + ": required "
                            + required
                            + " bytes but only "
                            + buffer.remaining()
                            + " remain");
        }
    }

    /** Creates a consistent fail-fast exception for malformed server output. */
    private IllegalArgumentException malformed(String message) {
        return new IllegalArgumentException("Malformed mppdb_decoding binary payload: " + message);
    }
}
