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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hbase.state.HbaseCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hbase.state.HbaseSinkState;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HbaseSinkWriter
        implements SinkWriter<SeaTunnelRow, HbaseCommitInfo, HbaseSinkState>,
                SupportMultiTableSinkWriter<Void> {

    private static final String ALL_COLUMNS = "all_columns";

    private final HbaseClient hbaseClient;

    private final SeaTunnelRowType seaTunnelRowType;

    private final HbaseParameters hbaseParameters;

    private final Charset charset;

    private List<Integer> rowkeyColumnIndexes;

    private int versionColumnIndex;

    private String defaultFamilyName = "value";

    /** Column families this writer puts data into; a DELETE row only touches these. */
    private final List<byte[]> writtenFamilies;

    /**
     * Retains the first client failure that may have discarded buffered mutations. Both mutate and
     * flush can drain the buffer, so neither may be retried on this writer after that failure.
     */
    private IOException bufferedWriteFailure;

    public HbaseSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HbaseParameters hbaseParameters,
            List<Integer> rowkeyColumnIndexes,
            int versionColumnIndex) {
        this(seaTunnelRowType, hbaseParameters, rowkeyColumnIndexes, versionColumnIndex, null);
    }

    HbaseSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HbaseParameters hbaseParameters,
            List<Integer> rowkeyColumnIndexes,
            int versionColumnIndex,
            HbaseClient hbaseClient) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.hbaseParameters = hbaseParameters;
        this.charset = Charset.forName(hbaseParameters.getEnCoding().toString());
        this.rowkeyColumnIndexes = rowkeyColumnIndexes;
        this.versionColumnIndex = versionColumnIndex;

        if (hbaseParameters.getFamilyNames().size() == 1) {
            defaultFamilyName =
                    hbaseParameters.getFamilyNames().getOrDefault(ALL_COLUMNS, defaultFamilyName);
        }
        this.writtenFamilies =
                writeColumnIndexes(seaTunnelRowType.getTotalFields()).stream()
                        .map(seaTunnelRowType::getFieldName)
                        .map(this::familyNameOf)
                        .distinct()
                        .map(Bytes::toBytes)
                        .collect(Collectors.toList());

        this.hbaseClient =
                hbaseClient == null ? HbaseClient.createInstance(hbaseParameters) : hbaseClient;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        failIfUnusable();
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                mutate(convertRowToPut(element));
                break;
            case DELETE:
                mutate(convertRowToDelete(element));
                break;
            case UPDATE_BEFORE:
                // Before images are ignored. Images may reach different
                // replicas or checkpoints, so this writer cannot pair a key-changing update.
                break;
            default:
                break;
        }
    }

    /** Submits a mutation, which can also flush rows already accepted into the client buffer. */
    private void mutate(Mutation mutation) throws IOException {
        try {
            hbaseClient.mutate(mutation);
        } catch (IOException e) {
            bufferedWriteFailure = e;
            throw e;
        }
    }

    /**
     * Flushes the client buffer at a checkpoint. Keep the native HBase periodic flush disabled when
     * relying on checkpoint delivery because the timer can send mutations and report errors
     * independently of this writer. Without this explicit flush the {@code BufferedMutator} keeps
     * up to {@code write_buffer_size} bytes in memory, and a task failure after the checkpoint
     * would lose rows the engine considers written.
     */
    @Override
    public Optional<HbaseCommitInfo> prepareCommit() throws IOException {
        failIfUnusable();
        try {
            hbaseClient.flush();
        } catch (IOException e) {
            bufferedWriteFailure = e;
            throw e;
        }
        return Optional.empty();
    }

    /** Rejects further writes/checkpoints after a flush that may have discarded buffered rows. */
    private void failIfUnusable() throws IOException {
        if (bufferedWriteFailure != null) {
            throw new IOException(
                    "The HBase sink writer for table "
                            + hbaseParameters.getTable()
                            + " cannot continue after a client write failure because buffered rows"
                            + " may have been lost. Restore from the last successful checkpoint.",
                    bufferedWriteFailure);
        }
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        if (hbaseClient != null) {
            hbaseClient.close();
        }
    }

    /**
     * A DELETE row removes every version up to its timestamp of the column families this writer
     * writes to. Families the job does not map, for example ones owned by another pipeline on the
     * same table, are left alone. With no mapped family, HBase would interpret a marker-free
     * mutation as a whole-row delete, so reject it before submitting it.
     */
    private Delete convertRowToDelete(SeaTunnelRow row) {
        if (writtenFamilies.isEmpty()) {
            throw new HbaseConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Cannot apply a DELETE to HBase table "
                            + hbaseParameters.getTable()
                            + " with no mapped column families. Include a non-key payload column"
                            + " to scope the delete.");
        }
        long timestamp = mutationTimestamp(row);
        Delete delete = new Delete(getRowkeyFromRow(row), timestamp);
        for (byte[] family : writtenFamilies) {
            delete.addFamily(family, timestamp);
        }
        if (!hbaseParameters.isWalWrite()) {
            delete.setDurability(Durability.SKIP_WAL);
        }
        return delete;
    }

    private String familyNameOf(String fieldName) {
        return hbaseParameters.getFamilyNames().getOrDefault(fieldName, defaultFamilyName);
    }

    private List<Integer> writeColumnIndexes(int arity) {
        return IntStream.range(0, arity)
                .boxed()
                .filter(index -> !rowkeyColumnIndexes.contains(index))
                .filter(index -> index != versionColumnIndex)
                .collect(Collectors.toList());
    }

    /**
     * Uses the configured BIGINT epoch milliseconds, or the clock when no version column is set.
     */
    private long mutationTimestamp(SeaTunnelRow row) {
        if (versionColumnIndex == -1) {
            return System.currentTimeMillis();
        }
        Object version = row.getField(versionColumnIndex);
        if (!(version instanceof Long)) {
            throw new HbaseConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    String.format(
                            "version_column [%s] must hold a non-null BIGINT epoch millisecond"
                                    + " value, got [%s]",
                            seaTunnelRowType.getFieldName(versionColumnIndex), version));
        }
        return (Long) version;
    }

    private Put convertRowToPut(SeaTunnelRow row) {
        byte[] rowkey = getRowkeyFromRow(row);
        Put put = new Put(rowkey, mutationTimestamp(row));
        if (hbaseParameters.getTtl() != -1 && hbaseParameters.getTtl() > 0) {
            put.setTTL(hbaseParameters.getTtl());
        }
        if (!hbaseParameters.isWalWrite()) {
            put.setDurability(Durability.SKIP_WAL);
        }
        for (Integer writeColumnIndex : writeColumnIndexes(row.getArity())) {
            String fieldName = seaTunnelRowType.getFieldName(writeColumnIndex);
            String familyName = familyNameOf(fieldName);
            byte[] bytes = convertColumnToBytes(row, writeColumnIndex);
            if (bytes != null) {
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(fieldName), bytes);
            } else {
                switch (hbaseParameters.getNullMode()) {
                    case EMPTY:
                        put.addColumn(
                                Bytes.toBytes(familyName),
                                Bytes.toBytes(fieldName),
                                HConstants.EMPTY_BYTE_ARRAY);
                        break;
                    case SKIP:
                    default:
                        break;
                }
            }
        }
        return put;
    }

    private byte[] getRowkeyFromRow(SeaTunnelRow row) {
        int rowkeySize = rowkeyColumnIndexes.size();
        int firstRowkeyIndex = rowkeyColumnIndexes.get(0);
        if (rowkeySize == 1 && isBinaryRowkeyColumn(firstRowkeyIndex)) {
            return (byte[]) row.getField(firstRowkeyIndex);
        }
        if (!hasBinaryRowkeyColumn()) {
            String[] rowkeyValues = new String[rowkeySize];
            for (int i = 0; i < rowkeySize; i++) {
                rowkeyValues[i] = row.getField(rowkeyColumnIndexes.get(i)).toString();
            }
            return Bytes.toBytes(String.join(hbaseParameters.getRowkeyDelimiter(), rowkeyValues));
        }
        byte[] delimiter = Bytes.toBytes(hbaseParameters.getRowkeyDelimiter());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (int i = 0; i < rowkeySize; i++) {
            if (i > 0 && delimiter.length > 0) {
                output.write(delimiter, 0, delimiter.length);
            }
            byte[] bytes = rowkeyFieldToBytes(rowkeyColumnIndexes.get(i), row);
            output.write(bytes, 0, bytes.length);
        }
        return output.toByteArray();
    }

    private boolean hasBinaryRowkeyColumn() {
        for (Integer index : rowkeyColumnIndexes) {
            if (isBinaryRowkeyColumn(index)) {
                return true;
            }
        }
        return false;
    }

    private boolean isBinaryRowkeyColumn(int index) {
        return seaTunnelRowType.getFieldType(index).getSqlType() == SqlType.BYTES;
    }

    private byte[] rowkeyFieldToBytes(int index, SeaTunnelRow row) {
        if (isBinaryRowkeyColumn(index)) {
            return (byte[]) row.getField(index);
        }
        return Bytes.toBytes(row.getField(index).toString());
    }

    private byte[] convertColumnToBytes(SeaTunnelRow row, int index) {
        Object field = row.getField(index);
        if (field == null) {
            return null;
        }
        SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(index);
        switch (fieldType.getSqlType()) {
            case TINYINT:
                return Bytes.toBytes((Byte) field);
            case SMALLINT:
                return Bytes.toBytes((Short) field);
            case INT:
                return Bytes.toBytes((Integer) field);
            case BIGINT:
                return Bytes.toBytes((Long) field);
            case FLOAT:
                return Bytes.toBytes((Float) field);
            case DOUBLE:
                return Bytes.toBytes((Double) field);
            case BOOLEAN:
                return Bytes.toBytes((Boolean) field);
            case BYTES:
                return (byte[]) field;
            case DECIMAL:
                BigDecimal decimal =
                        field instanceof BigDecimal
                                ? (BigDecimal) field
                                : new BigDecimal(field.toString());
                return decimal.toPlainString().getBytes(charset);
            case DATE:
                LocalDate date =
                        field instanceof LocalDate
                                ? (LocalDate) field
                                : DateUtils.parse(field.toString());
                return DateUtils.toString(date, DateUtils.Formatter.YYYY_MM_DD).getBytes(charset);
            case TIME:
                LocalTime time =
                        field instanceof LocalTime
                                ? (LocalTime) field
                                : TimeUtils.parse(field.toString());
                return TimeUtils.toString(time, TimeUtils.Formatter.HH_MM_SS).getBytes(charset);
            case TIMESTAMP:
                LocalDateTime timestamp =
                        field instanceof LocalDateTime
                                ? (LocalDateTime) field
                                : DateTimeUtils.parse(field.toString());
                return DateTimeUtils.toString(
                                timestamp, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)
                        .getBytes(charset);
            case ARRAY:
                String arrayAsString = field.toString().replaceAll("\\[|\\]|\\s", "");
                return arrayAsString.getBytes(charset);
            case STRING:
                return field.toString().getBytes(charset);
            default:
                String errorMsg =
                        String.format(
                                "Hbase connector does not support this column type [%s]",
                                fieldType.getSqlType());
                throw new HbaseConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}
