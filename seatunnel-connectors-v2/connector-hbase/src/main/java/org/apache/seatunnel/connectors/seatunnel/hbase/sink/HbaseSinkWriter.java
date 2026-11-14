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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
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

    /** The last UPDATE_BEFORE row, kept until its UPDATE_AFTER tells whether the row key moved. */
    private SeaTunnelRow pendingUpdateBefore;

    /**
     * Set once a flush failed. The HBase client drops the mutations of a failed flush, so a later
     * flush on the same writer would succeed with those rows gone; the writer refuses to go on.
     */
    private IOException flushFailure;

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
                pendingUpdateBefore = null;
                hbaseClient.mutate(convertRowToPut(element));
                break;
            case UPDATE_AFTER:
                deleteMovedRow(element);
                hbaseClient.mutate(convertRowToPut(element));
                break;
            case DELETE:
                pendingUpdateBefore = null;
                hbaseClient.mutate(convertRowToDelete(element));
                break;
            case UPDATE_BEFORE:
                // Not written on its own: the UPDATE_AFTER that follows overwrites the same row
                // key, and a tombstone written here in the same millisecond would hide that Put.
                // The row is kept so that an update which moves the row key can drop the old row.
                pendingUpdateBefore = element;
                break;
            default:
                break;
        }
    }

    /** When the UPDATE_AFTER carries another row key than its UPDATE_BEFORE, delete the old row. */
    private void deleteMovedRow(SeaTunnelRow updateAfter) throws IOException {
        SeaTunnelRow before = pendingUpdateBefore;
        pendingUpdateBefore = null;
        if (before != null
                && !Arrays.equals(getRowkeyFromRow(before), getRowkeyFromRow(updateAfter))) {
            hbaseClient.mutate(convertRowToDelete(before));
        }
    }

    /**
     * Pushes every buffered mutation to the region servers before the checkpoint is acknowledged.
     * Without this the {@code BufferedMutator} keeps up to {@code write_buffer_size} bytes in
     * memory, and a task failure after the checkpoint would lose rows the engine considers written.
     */
    @Override
    public Optional<HbaseCommitInfo> prepareCommit() throws IOException {
        failIfUnusable();
        try {
            hbaseClient.flush();
        } catch (IOException e) {
            flushFailure = e;
            throw e;
        }
        return Optional.empty();
    }

    private void failIfUnusable() throws IOException {
        if (flushFailure != null) {
            throw new IOException(
                    "The HBase sink writer cannot continue after a failed flush: the client dropped"
                            + " the rows of that flush, and only a restore from the last checkpoint"
                            + " brings them back",
                    flushFailure);
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
     * same table, are left alone. A row type with no data columns falls back to a whole-row delete.
     */
    private Delete convertRowToDelete(SeaTunnelRow row) {
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
