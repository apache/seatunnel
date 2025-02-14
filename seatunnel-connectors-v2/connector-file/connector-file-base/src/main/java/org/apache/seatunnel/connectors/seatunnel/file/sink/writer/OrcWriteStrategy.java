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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import lombok.NonNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OrcWriteStrategy extends AbstractWriteStrategy<Writer> {
    private final LinkedHashMap<String, Writer> beingWrittenWriter;

    public OrcWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        Writer writer = getOrCreateOutputStream(filePath);
        TypeDescription schema = buildSchemaWithRowType();
        VectorizedRowBatch rowBatch = schema.createRowBatch();
        int i = 0;
        int row = rowBatch.size++;
        for (Integer index : sinkColumnsIndexInRow) {
            Object value = seaTunnelRow.getField(index);
            ColumnVector vector = rowBatch.cols[i];
            setColumn(value, vector, row);
            i++;
        }
        try {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("OrcFile", "write", filePath, e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    try {
                        v.close();
                    } catch (IOException e) {
                        String errorMsg =
                                String.format(
                                        "Close file [%s] orc writer failed, error msg: [%s]",
                                        k, e.getMessage());
                        throw new FileConnectorException(
                                CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, errorMsg, e);
                    }
                    needMoveFiles.put(k, getTargetLocation(k));
                });
        this.beingWrittenWriter.clear();
    }

    @Override
    public Writer getOrCreateOutputStream(@NonNull String filePath) {
        Writer writer = this.beingWrittenWriter.get(filePath);
        if (writer == null) {
            TypeDescription schema = buildSchemaWithRowType();
            Path path = new Path(filePath);
            try {
                OrcFile.WriterOptions options =
                        OrcFile.writerOptions(getConfiguration(hadoopConf))
                                .setSchema(schema)
                                .compress(compressFormat.getOrcCompression())
                                // use orc version 0.12
                                .version(OrcFile.Version.V_0_12)
                                .fileSystem(hadoopFileSystemProxy.getFileSystem())
                                .overwrite(true);
                Writer newWriter = OrcFile.createWriter(path, options);
                this.beingWrittenWriter.put(filePath, newWriter);
                return newWriter;
            } catch (IOException e) {
                String errorMsg = String.format("Get orc writer for file [%s] error", filePath);
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, errorMsg, e);
            }
        }
        return writer;
    }

    public static TypeDescription buildFieldWithRowType(SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) type).getElementType();
                return TypeDescription.createList(buildFieldWithRowType(elementType));
            case MAP:
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) type).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) type).getValueType();
                return TypeDescription.createMap(
                        buildFieldWithRowType(keyType), buildFieldWithRowType(valueType));
            case STRING:
                return TypeDescription.createString();
            case BOOLEAN:
                return TypeDescription.createBoolean();
            case TINYINT:
                return TypeDescription.createByte();
            case SMALLINT:
                return TypeDescription.createShort();
            case INT:
                return TypeDescription.createInt();
            case BIGINT:
                return TypeDescription.createLong();
            case FLOAT:
                return TypeDescription.createFloat();
            case DOUBLE:
                return TypeDescription.createDouble();
            case DECIMAL:
                int precision = ((DecimalType) type).getPrecision();
                int scale = ((DecimalType) type).getScale();
                return TypeDescription.createDecimal().withScale(scale).withPrecision(precision);
            case BYTES:
                return TypeDescription.createBinary();
            case DATE:
                return TypeDescription.createDate();
            case TIME:
            case TIMESTAMP:
                return TypeDescription.createTimestamp();
            case ROW:
                TypeDescription struct = TypeDescription.createStruct();
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) type).getFieldTypes();
                for (int i = 0; i < fieldTypes.length; i++) {
                    struct.addField(
                            ((SeaTunnelRowType) type).getFieldName(i).toLowerCase(),
                            buildFieldWithRowType(fieldTypes[i]));
                }
                return struct;
            case NULL:
            default:
                String errorMsg =
                        String.format("Orc file not support this type [%s]", type.getSqlType());
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private TypeDescription buildSchemaWithRowType() {
        TypeDescription schema = TypeDescription.createStruct();
        for (Integer i : sinkColumnsIndexInRow) {
            TypeDescription fieldType = buildFieldWithRowType(seaTunnelRowType.getFieldType(i));
            schema.addField(seaTunnelRowType.getFieldName(i).toLowerCase(), fieldType);
        }
        return schema;
    }

    private void setColumn(Object value, ColumnVector vector, int row) {
        if (value == null) {
            vector.isNull[row] = true;
            vector.noNulls = false;
        } else {
            switch (vector.type) {
                case LONG:
                    LongColumnVector longVector = (LongColumnVector) vector;
                    setLongColumnVector(value, longVector, row);
                    break;
                case DOUBLE:
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) vector;
                    setDoubleVector(value, doubleColumnVector, row);
                    break;
                case BYTES:
                    BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
                    setByteColumnVector(value, bytesColumnVector, row);
                    break;
                case DECIMAL:
                    DecimalColumnVector decimalColumnVector = (DecimalColumnVector) vector;
                    setDecimalColumnVector(value, decimalColumnVector, row);
                    break;
                case TIMESTAMP:
                    TimestampColumnVector timestampColumnVector = (TimestampColumnVector) vector;
                    setTimestampColumnVector(value, timestampColumnVector, row);
                    break;
                case LIST:
                    ListColumnVector listColumnVector = (ListColumnVector) vector;
                    setListColumnVector(value, listColumnVector, row);
                    break;
                case MAP:
                    MapColumnVector mapColumnVector = (MapColumnVector) vector;
                    setMapColumnVector(value, mapColumnVector, row);
                    break;
                case STRUCT:
                    StructColumnVector structColumnVector = (StructColumnVector) vector;
                    setStructColumnVector(value, structColumnVector, row);
                    break;
                default:
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "Unsupported ColumnVector subtype" + vector.type);
            }
        }
    }

    private void setStructColumnVector(
            Object value, StructColumnVector structColumnVector, int row) {
        if (value instanceof SeaTunnelRow) {
            SeaTunnelRow seaTunnelRow = (SeaTunnelRow) value;
            Object[] fields = seaTunnelRow.getFields();
            for (int i = 0; i < fields.length; i++) {
                setColumn(fields[i], structColumnVector.fields[i], row);
            }
        } else {
            String errorMsg =
                    String.format(
                            "SeaTunnelRow type expected for field, "
                                    + "not support this data type: [%s]",
                            value.getClass());
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private void setMapColumnVector(Object value, MapColumnVector mapColumnVector, int row) {
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;

            mapColumnVector.offsets[row] = mapColumnVector.childCount;
            mapColumnVector.lengths[row] = map.size();
            mapColumnVector.childCount += map.size();

            int i = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                int mapElem = (int) mapColumnVector.offsets[row] + i;
                setColumn(entry.getKey(), mapColumnVector.keys, mapElem);
                setColumn(entry.getValue(), mapColumnVector.values, mapElem);
                ++i;
            }
        } else {
            String errorMsg =
                    String.format(
                            "Map type expected for field, this field is [%s]", value.getClass());
            throw new FileConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    private void setListColumnVector(Object value, ListColumnVector listColumnVector, int row) {
        Object[] valueArray;
        if (value instanceof Object[]) {
            valueArray = (Object[]) value;
        } else if (value instanceof List) {
            valueArray = ((List<?>) value).toArray();
        } else {
            String errorMsg =
                    String.format(
                            "List and Array type expected for field, " + "this field is [%s]",
                            value.getClass());
            throw new FileConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, errorMsg);
        }
        listColumnVector.offsets[row] = listColumnVector.childCount;
        listColumnVector.lengths[row] = valueArray.length;
        listColumnVector.childCount += valueArray.length;

        for (int i = 0; i < valueArray.length; i++) {
            int listElem = (int) listColumnVector.offsets[row] + i;
            setColumn(valueArray[i], listColumnVector.child, listElem);
        }
    }

    private void setDecimalColumnVector(
            Object value, DecimalColumnVector decimalColumnVector, int row) {
        if (value instanceof BigDecimal) {
            decimalColumnVector.set(row, HiveDecimal.create((BigDecimal) value));
        } else {
            String errorMsg =
                    String.format(
                            "BigDecimal type expected for field, this field is [%s]",
                            value.getClass());
            throw new FileConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    private void setTimestampColumnVector(
            Object value, TimestampColumnVector timestampColumnVector, int row) {
        if (value instanceof Timestamp) {
            timestampColumnVector.set(row, (Timestamp) value);
        } else if (value instanceof LocalDateTime) {
            timestampColumnVector.set(row, Timestamp.valueOf((LocalDateTime) value));
        } else if (value instanceof LocalTime) {
            timestampColumnVector.set(
                    row, Timestamp.valueOf(((LocalTime) value).atDate(LocalDate.ofEpochDay(0))));
        } else {
            String errorMsg =
                    String.format(
                            "Time series type expected for field, this field is [%s]",
                            value.getClass());
            throw new FileConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    private void setLongColumnVector(Object value, LongColumnVector longVector, int row) {
        if (value instanceof Boolean) {
            Boolean bool = (Boolean) value;
            longVector.vector[row] =
                    (bool.equals(Boolean.TRUE)) ? Long.valueOf(1) : Long.valueOf(0);
        } else if (value instanceof Integer) {
            longVector.vector[row] = ((Integer) value).longValue();
        } else if (value instanceof Long) {
            longVector.vector[row] = (Long) value;
        } else if (value instanceof BigInteger) {
            BigInteger bigInt = (BigInteger) value;
            longVector.vector[row] = bigInt.longValue();
        } else if (value instanceof Byte) {
            longVector.vector[row] = (Byte) value;
        } else if (value instanceof Short) {
            longVector.vector[row] = (Short) value;
        } else if (value instanceof LocalDate) {
            longVector.vector[row] = ((LocalDate) value).getLong(ChronoField.EPOCH_DAY);
        } else {
            String errorMsg =
                    String.format(
                            "Long or Integer type expected for field, " + "this field is [%s]",
                            value.getClass());
            throw new FileConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    private void setByteColumnVector(Object value, BytesColumnVector bytesColVector, int rowNum) {
        byte[] byteVec;
        if (value instanceof byte[]) {
            byteVec = (byte[]) value;
        } else {
            String strVal = value.toString();
            byteVec = strVal.getBytes(StandardCharsets.UTF_8);
        }
        bytesColVector.setRef(rowNum, byteVec, 0, byteVec.length);
    }

    private void setDoubleVector(Object value, DoubleColumnVector doubleVector, int rowNum) {
        if (value instanceof Double) {
            doubleVector.vector[rowNum] = (Double) value;
        } else if (value instanceof Float) {
            Float floatValue = (Float) value;
            doubleVector.vector[rowNum] = floatValue.doubleValue();
        } else {
            String errorMsg =
                    String.format(
                            "Double or Float type expected for field, " + "this field is [%s]",
                            value.getClass());
            throw new FileConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, errorMsg);
        }
    }
}
