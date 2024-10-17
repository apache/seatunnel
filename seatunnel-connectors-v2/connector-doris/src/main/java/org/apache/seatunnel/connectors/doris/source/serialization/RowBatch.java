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
import org.apache.seatunnel.shade.org.apache.arrow.vector.DateDayVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.DecimalVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FieldVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.Float4Vector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.Float8Vector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.IntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.SmallIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.TinyIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VarCharVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.ListVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.MapVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.StructVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types.MinorType;
import org.apache.seatunnel.shade.org.apache.arrow.vector.util.Text;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import org.apache.doris.sdk.thrift.TScanBatchResult;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

@Slf4j
public class RowBatch {
    SeaTunnelDataType<?>[] fieldTypes;
    private final ArrowStreamReader arrowStreamReader;
    private final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeV2Formatter =
            DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<SeaTunnelRow> seatunnelRowBatch = new ArrayList<>();
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;

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
                // Adapt unique model hidden columns
                for (int i = 0; i < fieldVectors.size(); i++) {
                    String fieldName = fieldVectors.get(i).getField().getName();
                    if (fieldName.equals("__DORIS_DELETE_SIGN__")) {
                        fieldVectors.remove(fieldVectors.get(i));
                    }
                }
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
        } catch (Throwable e) {
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
                convertArrowValue(col, currentType, dataType, minorType, fieldVector);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    private void convertArrowValue(
            int col,
            String currentType,
            SeaTunnelDataType<?> dataType,
            MinorType minorType,
            FieldVector fieldVector) {
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
                if (fieldVector instanceof BitVector) {
                    BitVector bv = (BitVector) fieldVector;
                    Preconditions.checkArgument(
                            minorType.equals(Types.MinorType.BIT),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col, rowIndex -> bv.isNull(rowIndex) ? null : (short) bv.get(rowIndex));

                } else if (fieldVector instanceof TinyIntVector) {
                    TinyIntVector tv = (TinyIntVector) fieldVector;
                    Preconditions.checkArgument(
                            minorType.equals(MinorType.TINYINT),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col, rowIndex -> tv.isNull(rowIndex) ? null : (short) tv.get(rowIndex));

                } else {
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
                }
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
                // LARGEINT
                if (fieldVector instanceof FixedSizeBinaryVector) {
                    FixedSizeBinaryVector largeInitFixedSizeBinaryVector =
                            (FixedSizeBinaryVector) fieldVector;
                    Preconditions.checkArgument(
                            minorType.equals(Types.MinorType.FIXEDSIZEBINARY),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col,
                            rowIndex -> {
                                if (largeInitFixedSizeBinaryVector.isNull(rowIndex)) {
                                    return null;
                                }
                                byte[] bytes = largeInitFixedSizeBinaryVector.get(rowIndex);
                                int left = 0, right = bytes.length - 1;
                                while (left < right) {
                                    byte temp = bytes[left];
                                    bytes[left] = bytes[right];
                                    bytes[right] = temp;
                                    left++;
                                    right--;
                                }
                                return new BigDecimal(new BigInteger(bytes), 0);
                            });
                    break;
                } else if (fieldVector instanceof VarCharVector) {
                    VarCharVector varCharVector = (VarCharVector) fieldVector;
                    Preconditions.checkArgument(
                            minorType.equals(Types.MinorType.VARCHAR),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col,
                            rowIndex ->
                                    varCharVector.isNull(rowIndex)
                                            ? null
                                            : new BigDecimal(
                                                    new String(varCharVector.get(rowIndex))));
                    break;
                }
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
            case "DATEV2":
                if (fieldVector instanceof DateDayVector) {
                    DateDayVector dateVector = (DateDayVector) fieldVector;
                    Preconditions.checkArgument(
                            minorType.equals(Types.MinorType.DATEDAY),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col,
                            rowIndex -> {
                                if (dateVector.isNull(rowIndex)) {
                                    return null;
                                }
                                return LocalDate.ofEpochDay(dateVector.get(rowIndex));
                            });
                    break;
                }
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
                if (fieldVector instanceof TimeStampMicroVector) {
                    TimeStampMicroVector timestampVector = (TimeStampMicroVector) fieldVector;

                    addValueToRowForAllRows(
                            col,
                            rowIndex -> {
                                if (timestampVector.isNull(rowIndex)) {
                                    return null;
                                }
                                return getDateTimeFromVector(rowIndex, fieldVector);
                            });
                    break;
                }
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
                                int left = 0, right = bytes.length - 1;
                                while (left < right) {
                                    byte temp = bytes[left];
                                    bytes[left] = bytes[right];
                                    bytes[right] = temp;
                                    left++;
                                    right--;
                                }
                                return new BigInteger(bytes).toString();
                            });
                    break;
                } else if (fieldVector instanceof MapVector) {
                    MapVector mapVector = (MapVector) fieldVector;
                    UnionMapReader reader = mapVector.getReader();
                    Preconditions.checkArgument(
                            minorType.equals(MinorType.MAP),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col,
                            rowIndex -> {
                                if (mapVector.isNull(rowIndex)) {
                                    return null;
                                }
                                reader.setPosition(rowIndex);
                                Map<String, Object> mapValue = new HashMap<>();
                                while (reader.next()) {
                                    mapValue.put(
                                            reader.key().readObject().toString(),
                                            reader.value().readObject());
                                }
                                return mapValue.toString();
                            });
                } else if (fieldVector instanceof StructVector) {
                    StructVector structVector = (StructVector) fieldVector;
                    Preconditions.checkArgument(
                            minorType.equals(MinorType.STRUCT),
                            typeMismatchMessage(currentType, minorType));
                    addValueToRowForAllRows(
                            col,
                            rowIndex -> {
                                if (structVector.isNull(rowIndex)) {
                                    return null;
                                }
                                Map<String, ?> structValue = structVector.getObject(rowIndex);
                                return structValue.toString();
                            });
                } else if (fieldVector instanceof ListVector) {
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
                                return Arrays.toString(listVectorObject.toArray());
                            });
                } else {
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
                }
                break;
            case "ARRAY":
                ListVector listVector = (ListVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(Types.MinorType.LIST),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForArrayColumn(dataType, col, listVector);
                break;
            case "MAP":
                MapVector mapVector = (MapVector) fieldVector;
                UnionMapReader reader = mapVector.getReader();
                Preconditions.checkArgument(
                        minorType.equals(MinorType.MAP),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForMapColumn(dataType, col, mapVector, reader);

                break;
            case "STRUCT":
                StructVector structVector = (StructVector) fieldVector;
                Preconditions.checkArgument(
                        minorType.equals(MinorType.STRUCT),
                        typeMismatchMessage(currentType, minorType));
                addValueToRowForAllRows(
                        col,
                        rowIndex -> {
                            if (structVector.isNull(rowIndex)) {
                                return null;
                            }
                            Map<String, ?> structValue = structVector.getObject(rowIndex);
                            return structValue;
                        });
                break;
            default:
                String errMsg = "Unsupported type " + fieldTypes[col].getSqlType().name();
                log.error(errMsg);
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.ARROW_READ_FAILED, errMsg);
        }
    }

    private void addValueToRowForMapColumn(
            SeaTunnelDataType<?> dataType, int col, MapVector mapVector, UnionMapReader reader) {
        addValueToRowForAllRows(
                col,
                rowIndex -> {
                    if (mapVector.isNull(rowIndex)) {
                        return null;
                    }
                    reader.setPosition(rowIndex);
                    Map<Object, Object> mapValue = new HashMap<>();
                    MapType mapType = (MapType) dataType;
                    SqlType keyType = mapType.getKeyType().getSqlType();
                    SqlType valueType = mapType.getValueType().getSqlType();
                    while (reader.next()) {
                        mapValue.put(
                                getDataFromVector(reader.key().readObject(), keyType),
                                getDataFromVector(reader.value().readObject(), valueType));
                    }
                    return mapValue;
                });
    }

    private Object getDataFromVector(Object vectorObject, SqlType sqlType) {
        if (vectorObject instanceof Boolean) {
            return Boolean.valueOf(vectorObject.toString());
        }

        if (vectorObject instanceof Byte) {
            return Byte.valueOf(vectorObject.toString());
        }

        if (vectorObject instanceof Short) {
            return Short.valueOf(vectorObject.toString());
        }

        if (vectorObject instanceof Integer) {
            if (sqlType.equals(SqlType.DATE)) {
                return LocalDate.ofEpochDay((int) vectorObject);
            }
            return Integer.valueOf(vectorObject.toString());
        }

        if (vectorObject instanceof Long) {
            return Long.valueOf(vectorObject.toString());
        }

        if (vectorObject instanceof Float) {
            return Float.valueOf(vectorObject.toString());
        }

        if (vectorObject instanceof Double) {
            return Double.valueOf(vectorObject.toString());
        }

        if (vectorObject instanceof Text) {
            if (sqlType.equals(SqlType.TIMESTAMP)) {
                String stringValue = completeMilliseconds(vectorObject.toString());
                return LocalDateTime.parse(stringValue, dateTimeV2Formatter);
            } else if (sqlType.equals(SqlType.DATE)) {
                return LocalDate.parse(vectorObject.toString(), dateFormatter);
            } else if (sqlType.equals(SqlType.DECIMAL)) {
                return new BigDecimal(vectorObject.toString());
            }
            return vectorObject.toString();
        }

        if (vectorObject instanceof BigDecimal) {
            return new BigDecimal(vectorObject.toString());
        }

        if (vectorObject instanceof byte[] && sqlType.equals(SqlType.DECIMAL)) {
            byte[] bytes = (byte[]) vectorObject;
            int left = 0, right = bytes.length - 1;
            while (left < right) {
                byte temp = bytes[left];
                bytes[left] = bytes[right];
                bytes[right] = temp;
                left++;
                right--;
            }
            return new BigDecimal(new BigInteger(bytes), 0);
        }
        if (vectorObject instanceof LocalDate) {
            return DateUtils.parse(vectorObject.toString());
        }

        if (vectorObject instanceof LocalDateTime) {
            return DateTimeUtils.parse(vectorObject.toString());
        }

        return vectorObject.toString();
    }

    private void addValueToRowForArrayColumn(
            SeaTunnelDataType<?> dataType, int col, ListVector listVector) {
        SqlType eleSqlType = null;
        if (dataType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dataType;
            eleSqlType = arrayType.getElementType().getSqlType();
        }
        SqlType finalEleSqlType = eleSqlType;
        addValueToRowForAllRows(
                col,
                rowIndex -> {
                    if (listVector.isNull(rowIndex)) {
                        return null;
                    }
                    List<?> listVectorObject = listVector.getObject(rowIndex);
                    if (listVectorObject.get(0) instanceof Boolean) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(Boolean[]::new);
                    }

                    if (listVectorObject.get(0) instanceof Byte) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(Byte[]::new);
                    }

                    if (listVectorObject.get(0) instanceof Short) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(Short[]::new);
                    }

                    if (listVectorObject.get(0) instanceof Integer) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(Integer[]::new);
                    }

                    if (listVectorObject.get(0) instanceof Long) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(Long[]::new);
                    }

                    if (listVectorObject.get(0) instanceof Float) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(Float[]::new);
                    }

                    if (listVectorObject.get(0) instanceof Double) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(Double[]::new);
                    }

                    if (listVectorObject.get(0) instanceof Text) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(String[]::new);
                    }

                    if (listVectorObject.get(0) instanceof BigDecimal) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(BigDecimal[]::new);
                    }

                    if (listVectorObject.get(0) instanceof byte[]
                            && dataType instanceof DecimalArrayType) {
                        return listVectorObject.stream()
                                .map(x -> getDataFromVector(x, finalEleSqlType))
                                .toArray(BigDecimal[]::new);
                    }

                    return listVectorObject.toArray();
                });
    }

    private void addValueToRowForAllRows(int col, IntFunction<Object> function) {
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = function.apply(rowIndex);
            addValueToRow(rowIndex, col, fieldValue);
        }
    }

    public LocalDateTime getDateTimeFromVector(int rowIndex, FieldVector fieldVector) {
        TimeStampMicroVector vector = (TimeStampMicroVector) fieldVector;
        if (vector.isNull(rowIndex)) {
            return null;
        }
        long time = vector.get(rowIndex);
        Instant instant;
        // Check if it's a second-level timestamp
        if (time / 10_000_000_000L == 0) {
            instant = Instant.ofEpochSecond(time);
            // Check if it's a millisecond-level
        } else if (time / 10_000_000_000_000L == 0) {
            instant = Instant.ofEpochMilli(time);
            // Doris supports up to 6 decimal places (microseconds)
        } else {
            instant = Instant.ofEpochSecond(time / 1000_000L, time % 1000_000L * 1000L);
        }
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
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
