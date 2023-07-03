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

package org.apache.seatunnel.connectors.doris.source.serialization;

import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.BigIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.BitVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.DecimalVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FieldVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.Float4Vector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.Float8Vector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.IntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.SmallIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.TinyIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VarCharVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.ListVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import org.apache.doris.sdk.thrift.TScanBatchResult;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

@Slf4j
public class RowBatch {
    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    SeaTunnelDataType<?>[] fieldTypes;
    private List<SeaTunnelRow> seatunnelRowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeV2Formatter =
            DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public RowBatch(TScanBatchResult nextResult, SeaTunnelRowType seaTunnelRowType) {
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader =
                new ArrowStreamReader(
                        new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        this.offsetInRowBatch = 0;
        this.fieldTypes = seaTunnelRowType.getFieldTypes();
    }

    public RowBatch readArrow() {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();

                if (fieldVectors.size() != fieldTypes.length) {
                    log.error(
                            "Schema size '{}' is not equal to arrow field size '{}'.",
                            fieldVectors.size(),
                            fieldTypes.length);
                    throw new DorisConnectorException(
                            DorisConnectorErrorCode.ARROW_READ_FAILED,
                            "Load Doris data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    log.debug("One batch in arrow has no data.");
                    continue;
                }
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    seatunnelRowBatch.add((new SeaTunnelRow(fieldVectors.size())));
                }
                convertArrowToRowBatch();
                readRowCount += root.getRowCount();
            }
            return this;
        } catch (Exception e) {
            log.error("Read Doris Data failed because: ", e);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.ARROW_READ_FAILED, e.getMessage());
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        return offsetInRowBatch < readRowCount;
    }

    private void addValueToRow(int rowIndex, int colIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg =
                    "Get row offset: " + rowIndex + " larger than row size: " + rowCountInOneBatch;
            log.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        seatunnelRowBatch.get(readRowCount + rowIndex).setField(colIndex, obj);
    }

    public void convertArrowToRowBatch() throws DorisConnectorException {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                SeaTunnelDataType<?> dataType = fieldTypes[col];
                final String currentType = dataType.getSqlType().name();

                FieldVector fieldVector = fieldVectors.get(col);
                Types.MinorType minorType = fieldVector.getMinorType();
                convertArrowValue(col, currentType, minorType, fieldVector);
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private void convertArrowValue(
            int col, String currentType, Types.MinorType minorType, FieldVector fieldVector) {
        switch (currentType) {
            case "BOOLEAN":
                BitVector bitVector = (BitVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.BIT),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0);
                break;
            case "TINYINT":
                TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.TINYINT),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                tinyIntVector.isNull(rowIndex)
                                        ? null
                                        : tinyIntVector.get(rowIndex));
                break;
            case "SMALLINT":
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.SMALLINT),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                smallIntVector.isNull(rowIndex)
                                        ? null
                                        : smallIntVector.get(rowIndex));
                break;

            case "INT":
                IntVector intVector = (IntVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.INT),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex -> intVector.isNull(rowIndex) ? null : intVector.get(rowIndex));
                break;
            case "BIGINT":
                BigIntVector bigIntVector = (BigIntVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.BIGINT),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex));
                break;
            case "FLOAT":
                Float4Vector float4Vector = (Float4Vector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.FLOAT4),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex));
                break;
            case "DOUBLE":
                Float8Vector float8Vector = (Float8Vector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.FLOAT8),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex));
                break;
            case "DECIMAL":
                DecimalVector decimalVector = (DecimalVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.DECIMAL),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                decimalVector.isNull(rowIndex)
                                        ? null
                                        : decimalVector.getObject(rowIndex).stripTrailingZeros());
                break;
            case "DATE":
                VarCharVector dateVector = (VarCharVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.VARCHAR),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex -> {
                            if (dateVector.isNull(rowIndex)) {
                                return null;
                            }
                            String stringValue = new String(dateVector.get(rowIndex));
                            return LocalDate.parse(stringValue, dateFormatter);
                        });
                break;
            case "TIMESTAMP":
                VarCharVector timestampVector = (VarCharVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.VARCHAR),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex -> {
                            if (timestampVector.isNull(rowIndex)) {
                                return null;
                            }
                            String stringValue = new String(timestampVector.get(rowIndex));
                            stringValue = completeMilliseconds(stringValue);
                            return LocalDateTime.parse(stringValue, dateTimeV2Formatter);
                        });
                break;
            case "STRING":
                if (fieldVector instanceof FixedSizeBinaryVector) {
                    FixedSizeBinaryVector fixedSizeBinaryVector =
                            (FixedSizeBinaryVector) fieldVector;
                    Preconditions.checkArgument(
                            minorType.equals(Types.MinorType.FIXEDSIZEBINARY),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col,
                            rowIndex -> {
                                if (fixedSizeBinaryVector.isNull(rowIndex)) {
                                    return null;
                                }
                                byte[] bytes = fixedSizeBinaryVector.get(rowIndex);
                                new String(bytes, StandardCharsets.UTF_8);
                                BigInteger value = new BigInteger(bytes);
                                System.out.println(value);
                                return value.toString();
                            });
                    break;
                }
                VarCharVector varCharVector = (VarCharVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.VARCHAR),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex ->
                                varCharVector.isNull(rowIndex)
                                        ? null
                                        : new String(varCharVector.get(rowIndex)));
                break;
            case "ARRAY":
                ListVector listVector = (ListVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.LIST),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex -> {
                            if (listVector.isNull(rowIndex)) {
                                return null;
                            }
                            List<?> listVectorObject = listVector.getObject(rowIndex);
                            return listVectorObject.toArray();
                        });
                break;
            default:
                String errMsg = "Unsupported type " + fieldTypes[col].getSqlType().name();
                log.error(errMsg);
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.ARROW_READ_FAILED, errMsg);
        }
    }

    private void addValueToRowForAllRows(int col, IntFunction<Object> function) {
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = function.apply(rowIndex);
            addValueToRow(rowIndex, col, fieldValue);
        }
    }

    private String completeMilliseconds(String stringValue) {
        if (stringValue.length() == DATETIMEV2_PATTERN.length()) {
            return stringValue;
        }
        StringBuilder sb = new StringBuilder(stringValue);
        if (stringValue.length() == DATETIME_PATTERN.length()) {
            sb.append(".");
        }
        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
            sb.append(0);
        }
        return sb.toString();
    }

    public SeaTunnelRow next() {
        if (!hasNext()) {
            String errMsg =
                    "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            log.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return seatunnelRowBatch.get(offsetInRowBatch++);
    }

    private String typeMismatchMessage(final String flinkType, final Types.MinorType arrowType) {
        final String messageTemplate = "FLINK type is %1$s, but arrow type is %2$s.";
        return String.format(messageTemplate, flinkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.ROW_BATCH_GET_FAILED,
                    "Failed to close ArrowStreamReader",
                    ioe);
        }
    }
}
