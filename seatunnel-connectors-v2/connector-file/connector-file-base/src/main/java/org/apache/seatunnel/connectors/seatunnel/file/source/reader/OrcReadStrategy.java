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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FilePluginException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class OrcReadStrategy extends AbstractReadStrategy {

    private SeaTunnelRowType seaTunnelRowTypeInfo;
    private static final long MIN_SIZE = 16 * 1024;

    @Override
    public void read(String path, Collector<SeaTunnelRow> output) throws Exception {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            throw new Exception("Please check file type");
        }
        Configuration configuration = getConfiguration();
        Path filePath = new Path(path);
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(configuration);
        try (Reader reader = OrcFile.createReader(filePath, readerOptions)) {
            TypeDescription schema = reader.getSchema();
            List<TypeDescription> children = schema.getChildren();
            RecordReader rows = reader.rows();
            VectorizedRowBatch rowBatch = reader.getSchema().createRowBatch();
            while (rows.nextBatch(rowBatch)) {
                int num = 0;
                for (int i = 0; i < rowBatch.size; i++) {
                    int numCols = rowBatch.numCols;
                    Object[] fields = new Object[numCols];
                    ColumnVector[] cols = rowBatch.cols;
                    for (int j = 0; j < numCols; j++) {
                        if (cols[j] == null) {
                            fields[j] = null;
                        } else {
                            fields[j] = readColumn(cols[j], children.get(j), num);
                        }
                    }
                    output.collect(new SeaTunnelRow(fields));
                    num++;
                }
            }
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) throws FilePluginException {
        if (null != seaTunnelRowTypeInfo) {
            return seaTunnelRowTypeInfo;
        }
        Configuration configuration = getConfiguration(hadoopConf);
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(configuration);
        Path dstDir = new Path(path);
        try (Reader reader = OrcFile.createReader(dstDir, readerOptions)) {
            TypeDescription schema = reader.getSchema();
            String[] fields = new String[schema.getFieldNames().size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[schema.getFieldNames().size()];
            for (int i = 0; i < schema.getFieldNames().size(); i++) {
                fields[i] = schema.getFieldNames().get(i);
                types[i] = orcDataType2SeaTunnelDataType(schema.getChildren().get(i));
            }
            seaTunnelRowTypeInfo = new SeaTunnelRowType(fields, types);
            return seaTunnelRowTypeInfo;
        } catch (IOException e) {
            throw new FilePluginException("Create OrcReader Fail", e);
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    boolean checkFileType(String path) {
        try {
            boolean checkResult;
            Configuration configuration = getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            Path filePath = new Path(path);
            FSDataInputStream in = fileSystem.open(filePath);
            // try to get Postscript in orc file
            long size = fileSystem.getFileStatus(filePath).getLen();
            int readSize = (int) Math.min(size, MIN_SIZE);
            in.seek(size - readSize);
            ByteBuffer buffer = ByteBuffer.allocate(readSize);
            in.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            int psLen = buffer.get(readSize - 1) & 0xff;
            int len = OrcFile.MAGIC.length();
            if (psLen < len + 1) {
                in.close();
                return false;
            }
            int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1 - len;
            byte[] array = buffer.array();
            if (Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
                checkResult = true;
            } else {
                // If it isn't there, this may be the 0.11.0 version of ORC.
                // Read the first 3 bytes of the file to check for the header
                in.seek(0);
                byte[] header = new byte[len];
                in.readFully(header, 0, len);
                // if it isn't there, this isn't an ORC file
                checkResult = Text.decode(header, 0, len).equals(OrcFile.MAGIC);
            }
            in.close();
            return checkResult;
        } catch (FilePluginException | IOException e) {
            String errorMsg = String.format("Check orc file [%s] error", path);
            throw new RuntimeException(errorMsg, e);
        }
    }

    private SeaTunnelDataType<?> orcDataType2SeaTunnelDataType(TypeDescription typeDescription) {
        switch (typeDescription.getCategory()) {
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case BYTE:
            case SHORT:
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case STRING:
            case BINARY:
            case VARCHAR:
            case CHAR:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DECIMAL:
                int precision = typeDescription.getPrecision();
                int scale = typeDescription.getScale();
                return new DecimalType(precision, scale);
            case LIST:
                TypeDescription listType = typeDescription.getChildren().get(0);
                SeaTunnelDataType<?> seaTunnelDataType = orcDataType2SeaTunnelDataType(listType);
                if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.STRING_ARRAY_TYPE;
                } else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.BOOLEAN_ARRAY_TYPE;
                } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.BYTE_ARRAY_TYPE;
                } else if (BasicType.SHORT_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.SHORT_ARRAY_TYPE;
                } else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.INT_ARRAY_TYPE;
                } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.LONG_ARRAY_TYPE;
                } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.FLOAT_ARRAY_TYPE;
                } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                    return ArrayType.DOUBLE_ARRAY_TYPE;
                }
                String errorMsg = String.format("SeaTunnel array type not supported this genericType [%s] yet", seaTunnelDataType);
                throw new RuntimeException(errorMsg);
            case MAP:
                TypeDescription keyType = typeDescription.getChildren().get(0);
                TypeDescription valueType = typeDescription.getChildren().get(1);
                return new MapType<>(orcDataType2SeaTunnelDataType(keyType), orcDataType2SeaTunnelDataType(valueType));
            case STRUCT:
                List<TypeDescription> children = typeDescription.getChildren();
                String[] fieldNames = typeDescription.getFieldNames().toArray(new String[0]);
                SeaTunnelDataType<?>[] fieldTypes = children.stream().map(this::orcDataType2SeaTunnelDataType).toArray(SeaTunnelDataType<?>[]::new);
                return new SeaTunnelRowType(fieldNames, fieldTypes);
            case UNION:
                // TODO: How to explain this type using SeaTunnel data type
                throw new RuntimeException("SeaTunnel not supported orc [union] type yet");
            default:
                // do nothing
                // never get in there
                return null;
        }
    }

    private Object readColumn(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object columnObj = null;
        if (!colVec.isNull[rowNum]) {
            switch (colVec.type) {
                case LONG:
                    columnObj = readLongVal(colVec, colType, rowNum);
                    break;
                case DOUBLE:
                    columnObj = ((DoubleColumnVector) colVec).vector[rowNum];
                    break;
                case BYTES:
                    columnObj = readBytesVal(colVec, rowNum);
                    break;
                case DECIMAL:
                    columnObj = readDecimalVal(colVec, rowNum);
                    break;
                case TIMESTAMP:
                    columnObj = readTimestampVal(colVec, colType, rowNum);
                    break;
                case STRUCT:
                    columnObj = readStructVal(colVec, colType, rowNum);
                    break;
                case LIST:
                    columnObj = readListVal(colVec, colType, rowNum);
                    break;
                case MAP:
                    columnObj = readMapVal(colVec, colType, rowNum);
                    break;
                case UNION:
                    columnObj = readUnionVal(colVec, colType, rowNum);
                    break;
                default:
                    throw new RuntimeException(
                            "ReadColumn: unsupported ORC file column type: " + colVec.type.name()
                    );
            }
        }
        return columnObj;
    }

    private Object readLongVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object colObj = null;
        if (!colVec.isNull[rowNum]) {
            LongColumnVector longVec = (LongColumnVector) colVec;
            long longVal = longVec.vector[rowNum];
            colObj = longVal;
            if (colType.getCategory() == TypeDescription.Category.INT) {
                colObj = (int) longVal;
            } else if (colType.getCategory() == TypeDescription.Category.BOOLEAN) {
                colObj = longVal == 1 ? Boolean.TRUE : Boolean.FALSE;
            } else if (colType.getCategory() == TypeDescription.Category.DATE) {
                colObj = LocalDate.ofEpochDay(longVal);
            }
        }
        return colObj;
    }

    private Object readBytesVal(ColumnVector colVec, int rowNum) {
        Object bytesObj = null;
        if (!colVec.isNull[rowNum]) {
            BytesColumnVector bytesVector = (BytesColumnVector) colVec;
            bytesObj = bytesVector.toString(rowNum);
        }
        return bytesObj;
    }

    private Object readDecimalVal(ColumnVector colVec, int rowNum) {
        Object decimalObj = null;
        if (!colVec.isNull[rowNum]) {
            DecimalColumnVector decimalVec = (DecimalColumnVector) colVec;
            decimalObj = decimalVec.vector[rowNum].getHiveDecimal().bigDecimalValue();
        }
        return decimalObj;
    }

    private Object readTimestampVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object timestampVal = null;
        if (!colVec.isNull[rowNum]) {
            TimestampColumnVector timestampVec = (TimestampColumnVector) colVec;
            int nanos = timestampVec.nanos[rowNum];
            long millis = timestampVec.time[rowNum];
            Timestamp timestamp = new Timestamp(millis);
            timestamp.setNanos(nanos);
            timestampVal = timestamp.toLocalDateTime();
            if (colType.getCategory() == TypeDescription.Category.DATE) {
                timestampVal = LocalDate.ofEpochDay(timestamp.getTime());
            }
        }
        return timestampVal;
    }

    private Object readStructVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object structObj = null;
        if (!colVec.isNull[rowNum]) {
            StructColumnVector structVector = (StructColumnVector) colVec;
            ColumnVector[] fieldVec = structVector.fields;
            Object[] fieldValues = new Object[fieldVec.length];
            List<TypeDescription> fieldTypes = colType.getChildren();
            for (int i = 0; i < fieldVec.length; i++) {
                Object fieldObj = readColumn(fieldVec[i], fieldTypes.get(i), rowNum);
                fieldValues[i] = fieldObj;
            }
            structObj = new SeaTunnelRow(fieldValues);
        }
        return structObj;
    }

    private Object readMapVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Map<Object, Object> objMap = new HashMap<>();
        MapColumnVector mapVector = (MapColumnVector) colVec;
        if (checkMapColumnVectorTypes(mapVector)) {
            int mapSize = (int) mapVector.lengths[rowNum];
            int offset = (int) mapVector.offsets[rowNum];
            List<TypeDescription> mapTypes = colType.getChildren();
            TypeDescription keyType = mapTypes.get(0);
            TypeDescription valueType = mapTypes.get(1);
            ColumnVector keyChild = mapVector.keys;
            ColumnVector valueChild = mapVector.values;
            Object[] keyList = readMapVector(keyChild, keyType, offset, mapSize);
            Object[] valueList = readMapVector(valueChild, valueType, offset, mapSize);
            for (int i = 0; i < keyList.length; i++) {
                objMap.put(keyList[i], valueList[i]);
            }
        } else {
            throw new RuntimeException("readMapVal: unsupported key or value types");
        }
        return objMap;
    }

    private boolean checkMapColumnVectorTypes(MapColumnVector mapVector) {
        ColumnVector.Type keyType = mapVector.keys.type;
        ColumnVector.Type valueType = mapVector.values.type;
        return
                keyType == ColumnVector.Type.BYTES ||
                        keyType == ColumnVector.Type.LONG ||
                        keyType == ColumnVector.Type.DOUBLE
                        &&
                        valueType == ColumnVector.Type.LONG ||
                        valueType == ColumnVector.Type.DOUBLE ||
                        valueType == ColumnVector.Type.BYTES ||
                        valueType == ColumnVector.Type.DECIMAL ||
                        valueType == ColumnVector.Type.TIMESTAMP;
    }

    @SuppressWarnings("unchecked")
    private Object[] readMapVector(ColumnVector mapVector, TypeDescription childType, int offset, int numValues) {
        Object[] mapList;
        switch (mapVector.type) {
            case BYTES:
                mapList =
                        readBytesListVector(
                                (BytesColumnVector) mapVector,
                                childType,
                                offset,
                                numValues
                        );
                break;
            case LONG:
                mapList =
                        readLongListVector(
                                (LongColumnVector) mapVector,
                                childType,
                                offset,
                                numValues
                        );
                break;
            case DOUBLE:
                mapList =
                        readDoubleListVector(
                                (DoubleColumnVector) mapVector,
                                offset,
                                numValues
                        );
                break;
            case DECIMAL:
                mapList =
                        readDecimalListVector(
                                (DecimalColumnVector) mapVector,
                                offset,
                                numValues
                        );
                break;
            case TIMESTAMP:
                mapList =
                        readTimestampListVector(
                                (TimestampColumnVector) mapVector,
                                childType,
                                offset,
                                numValues
                        );
                break;
            default:
                throw new RuntimeException(mapVector.type.name() + " is not supported for MapColumnVectors");
        }
        return mapList;
    }

    private Object readUnionVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Pair<TypeDescription, Object> columnValuePair;
        UnionColumnVector unionVector = (UnionColumnVector) colVec;
        int tagVal = unionVector.tags[rowNum];
        List<TypeDescription> unionFieldTypes = colType.getChildren();
        if (tagVal < unionFieldTypes.size()) {
            TypeDescription fieldType = unionFieldTypes.get(tagVal);
            if (tagVal < unionVector.fields.length) {
                ColumnVector fieldVector = unionVector.fields[tagVal];
                Object unionValue = readColumn(fieldVector, fieldType, rowNum);
                columnValuePair = Pair.of(fieldType, unionValue);
            } else {
                throw new RuntimeException(
                        "readUnionVal: union tag value out of range for union column vectors"
                );
            }
        } else {
            throw new RuntimeException(
                    "readUnionVal: union tag value out of range for union types"
            );
        }
        return columnValuePair;
    }

    private Object readListVal(ColumnVector colVec, TypeDescription colType, int rowNum) {
        Object listValues = null;
        if (!colVec.isNull[rowNum]) {
            ListColumnVector listVector = (ListColumnVector) colVec;
            ColumnVector listChildVector = listVector.child;
            TypeDescription childType = colType.getChildren().get(0);
            switch (listChildVector.type) {
                case LONG:
                    listValues = readLongListValues(listVector, childType, rowNum);
                    break;
                case DOUBLE:
                    listValues = readDoubleListValues(listVector, rowNum);
                    break;
                case BYTES:
                    listValues = readBytesListValues(listVector, childType, rowNum);
                    break;
                case DECIMAL:
                    listValues = readDecimalListValues(listVector, rowNum);
                    break;
                case TIMESTAMP:
                    listValues = readTimestampListValues(listVector, childType, rowNum);
                    break;
                default:
                    throw new RuntimeException(
                            listVector.type.name() + " is not supported for ListColumnVectors"
                    );
            }
        }
        return listValues;
    }

    private Object readLongListValues(ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        LongColumnVector longVector = (LongColumnVector) listVector.child;
        return readLongListVector(longVector, childType, offset, numValues);
    }

    private Object[] readLongListVector(LongColumnVector longVector, TypeDescription childType, int offset, int numValues) {
        List<Object> longList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!longVector.isNull[offset + i]) {
                long longVal = longVector.vector[offset + i];
                if (childType.getCategory() == TypeDescription.Category.BOOLEAN) {
                    Boolean boolVal = longVal == 0 ? Boolean.valueOf(false) : Boolean.valueOf(true);
                    longList.add(boolVal);
                } else if (childType.getCategory() == TypeDescription.Category.INT) {
                    Integer intObj = (int) longVal;
                    longList.add(intObj);
                } else {
                    longList.add(longVal);
                }
            } else {
                longList.add(null);
            }
        }
        return longList.toArray();
    }

    private Object readDoubleListValues(ListColumnVector listVector, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        DoubleColumnVector doubleVec = (DoubleColumnVector) listVector.child;
        return readDoubleListVector(doubleVec, offset, numValues);
    }

    private Object[] readDoubleListVector(DoubleColumnVector doubleVec, int offset, int numValues) {
        List<Object> doubleList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!doubleVec.isNull[offset + i]) {
                Double doubleVal = doubleVec.vector[offset + i];
                doubleList.add(doubleVal);
            } else {
                doubleList.add(null);
            }
        }
        return doubleList.toArray();
    }

    private Object readBytesListValues(ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        BytesColumnVector bytesVec = (BytesColumnVector) listVector.child;
        return readBytesListVector(bytesVec, childType, offset, numValues);
    }

    private Object[] readBytesListVector(BytesColumnVector bytesVec, TypeDescription childType, int offset, int numValues) {
        List<Object> bytesValList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!bytesVec.isNull[offset + i]) {
                byte[] byteArray = bytesVec.vector[offset + i];
                int vecLen = bytesVec.length[offset + i];
                int vecStart = bytesVec.start[offset + i];
                byte[] vecCopy = Arrays.copyOfRange(byteArray, vecStart, vecStart + vecLen);
                if (childType.getCategory() == TypeDescription.Category.STRING) {
                    String str = new String(vecCopy);
                    bytesValList.add(str);
                } else {
                    bytesValList.add(vecCopy);
                }
            } else {
                bytesValList.add(null);
            }
        }
        return bytesValList.toArray(new String[0]);
    }

    private Object readDecimalListValues(ListColumnVector listVector, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        DecimalColumnVector decimalVec = (DecimalColumnVector) listVector.child;
        return readDecimalListVector(decimalVec, offset, numValues);
    }

    private Object[] readDecimalListVector(DecimalColumnVector decimalVector, int offset, int numValues) {
        List<Object> decimalList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!decimalVector.isNull[offset + i]) {
                BigDecimal bigDecimal = decimalVector.vector[i].getHiveDecimal().bigDecimalValue();
                decimalList.add(bigDecimal);
            } else {
                decimalList.add(null);
            }
        }
        return decimalList.toArray();
    }

    private Object readTimestampListValues(ListColumnVector listVector, TypeDescription childType, int rowNum) {
        int offset = (int) listVector.offsets[rowNum];
        int numValues = (int) listVector.lengths[rowNum];
        TimestampColumnVector timestampVec = (TimestampColumnVector) listVector.child;
        return readTimestampListVector(timestampVec, childType, offset, numValues);
    }

    private Object[] readTimestampListVector(TimestampColumnVector timestampVector, TypeDescription childType, int offset, int numValues) {
        List<Object> timestampList = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            if (!timestampVector.isNull[offset + i]) {
                int nanos = timestampVector.nanos[offset + i];
                long millis = timestampVector.time[offset + i];
                Timestamp timestamp = new Timestamp(millis);
                timestamp.setNanos(nanos);
                if (childType.getCategory() == TypeDescription.Category.DATE) {
                    LocalDate localDate = LocalDate.ofEpochDay(timestamp.getTime());
                    timestampList.add(localDate);
                } else {
                    timestampList.add(timestamp.toLocalDateTime());
                }
            } else {
                timestampList.add(null);
            }
        }
        return timestampList.toArray();
    }
}

