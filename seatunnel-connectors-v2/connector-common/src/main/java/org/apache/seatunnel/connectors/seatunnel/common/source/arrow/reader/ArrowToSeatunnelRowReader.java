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

package org.apache.seatunnel.connectors.seatunnel.common.source.arrow.reader;

import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FieldVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;
import org.apache.seatunnel.shade.org.apache.arrow.vector.util.Text;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.common.source.arrow.converter.Converter;
import org.apache.seatunnel.connectors.seatunnel.common.source.arrow.converter.DefaultConverter;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

@Slf4j
public class ArrowToSeatunnelRowReader implements AutoCloseable {

    private final SeaTunnelDataType<?>[] seaTunnelDataTypes;
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<FieldVector> fieldVectors;
    private VectorSchemaRoot root;
    private ArrowStreamReader arrowStreamReader;
    private RootAllocator rootAllocator;
    private final Map<String, Integer> fieldIndexMap = new HashMap<>();
    private final List<SeaTunnelRow> seatunnelRowBatch = new ArrayList<>();
    private static final List<Converter> converters = new ArrayList<>();
    private final DefaultConverter defaultConverter = new DefaultConverter();
    private final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        ServiceLoader.load(Converter.class).forEach(converters::add);
    }

    public ArrowToSeatunnelRowReader(byte[] byteArray, SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelDataTypes = seaTunnelRowType.getFieldTypes();
        initFieldIndexMap(seaTunnelRowType);
        initArrowReader(byteArray);
    }

    private void initFieldIndexMap(SeaTunnelRowType seaTunnelRowType) {
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            fieldIndexMap.put(seaTunnelRowType.getFieldNames()[i], i);
        }
    }

    private void initArrowReader(byte[] byteArray) {
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader =
                new ArrowStreamReader(new ByteArrayInputStream(byteArray), rootAllocator);
    }

    public ArrowToSeatunnelRowReader readArrow() {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                this.fieldVectors = root.getFieldVectors();
                if (fieldVectors.isEmpty() || root.getRowCount() == 0) {
                    log.debug("one batch in arrow has no data.");
                    continue;
                }
                log.info("one batch in arrow row count size '{}'", root.getRowCount());
                this.rowCountInOneBatch = root.getRowCount();
                for (int i = 0; i < rowCountInOneBatch; i++) {
                    seatunnelRowBatch.add(new SeaTunnelRow(this.seaTunnelDataTypes.length));
                }
                convertSeatunnelRow();
                this.readRowCount += root.getRowCount();
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        return offsetInRowBatch < readRowCount;
    }

    public SeaTunnelRow next() {
        if (!hasNext()) {
            throw new IllegalStateException("no more rows to read.");
        }
        return seatunnelRowBatch.get(offsetInRowBatch++);
    }

    private void convertSeatunnelRow() {
        for (FieldVector fieldVector : fieldVectors) {
            String name = fieldVector.getField().getName();
            Integer fieldIndex = fieldIndexMap.get(name);
            Types.MinorType minorType = fieldVector.getMinorType();
            for (int i = 0; i < seatunnelRowBatch.size(); i++) {
                // arrow field not in the Seatunnel Schema field, skip it
                if (fieldIndex != null) {
                    SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[fieldIndex];
                    Object fieldValue =
                            convertArrowData(
                                    readRowCount + i, minorType, fieldVector, seaTunnelDataType);
                    fieldValue =
                            convertSeatunnelRowValue(
                                    seaTunnelDataType.getSqlType(), minorType, fieldValue);
                    seatunnelRowBatch.get(readRowCount + i).setField(fieldIndex, fieldValue);
                }
            }
        }
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    private Object convertSeatunnelRowValue(
            SqlType currentType, Types.MinorType minorType, Object fieldValue) {
        switch (currentType) {
            case STRING:
                if (fieldValue instanceof byte[]) {
                    return new String((byte[]) fieldValue);
                } else if (fieldValue instanceof Text) {
                    return ((Text) fieldValue).toString();
                } else {
                    return fieldValue;
                }
            case DECIMAL:
                if (fieldValue instanceof String) {
                    return new BigDecimal((String) fieldValue);
                } else if (fieldValue instanceof Text) {
                    return new BigDecimal(((Text) fieldValue).toString());
                } else {
                    return fieldValue;
                }
            case DATE:
                if (fieldValue instanceof Integer) {
                    return LocalDate.ofEpochDay((Integer) fieldValue);
                } else if (fieldValue instanceof Long) {
                    return LocalDate.ofEpochDay((Long) fieldValue);
                } else if (fieldValue instanceof String) {
                    return LocalDate.parse((String) fieldValue, DATE_FORMATTER);
                } else if (fieldValue instanceof Text) {
                    return LocalDate.parse(((Text) fieldValue).toString(), DATE_FORMATTER);
                } else if (fieldValue instanceof LocalDateTime) {
                    return ((LocalDateTime) fieldValue).toLocalDate();
                } else {
                    return fieldValue;
                }
            case TIME:
                if (fieldValue instanceof Integer) {
                    return LocalTime.ofSecondOfDay((Integer) fieldValue);
                } else if (fieldValue instanceof Long) {
                    return Instant.ofEpochMilli((Long) fieldValue)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime()
                            .toLocalTime();
                } else if (fieldValue instanceof String) {
                    return LocalTime.parse((String) fieldValue, TIME_FORMATTER);
                } else if (fieldValue instanceof Text) {
                    return LocalTime.parse(((Text) fieldValue).toString(), TIME_FORMATTER);
                } else {
                    return fieldValue;
                }
            case TIMESTAMP:
                if (fieldValue instanceof Long) {
                    return Instant.ofEpochMilli((Long) fieldValue)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime();
                } else if (fieldValue instanceof String) {
                    return LocalDateTime.parse((String) fieldValue, DATETIME_FORMATTER);
                } else if (fieldValue instanceof Text) {
                    return LocalDateTime.parse(((Text) fieldValue).toString(), DATETIME_FORMATTER);
                } else {
                    return fieldValue;
                }
            default:
                return fieldValue;
        }
    }

    private Object convertArrowData(
            int rowIndex,
            Types.MinorType minorType,
            FieldVector fieldVector,
            SeaTunnelDataType<?> seaTunnelDataType) {
        if (seaTunnelDataType == null) {
            throw new IllegalArgumentException("seaTunnelDataType cannot be null");
        }

        for (Converter converter : converters) {
            if (converter.support(minorType)) {
                SqlType sqlType = seaTunnelDataType.getSqlType();
                switch (sqlType) {
                    case MAP:
                        return convertMap(
                                rowIndex, converter, fieldVector, (MapType) seaTunnelDataType);
                    case ARRAY:
                        return convertArray(
                                rowIndex, converter, fieldVector, (ArrayType) seaTunnelDataType);
                    case ROW:
                        return convertRow(
                                rowIndex,
                                converter,
                                fieldVector,
                                (SeaTunnelRowType) seaTunnelDataType);
                    default:
                        return converter.convert(rowIndex, fieldVector);
                }
            }
        }
        return defaultConverter.convert(rowIndex, fieldVector);
    }

    private Object convertMap(
            int rowIndex, Converter converter, FieldVector fieldVector, MapType mapType) {
        SqlType keyType = mapType.getKeyType().getSqlType();
        SqlType valueType = mapType.getValueType().getSqlType();
        Map<String, Function> fieldConverters = new HashMap<>();
        fieldConverters.put(Converter.MAP_KEY, genericsConvert(keyType));
        fieldConverters.put(Converter.MAP_VALUE, genericsConvert(valueType));
        return converter.convert(rowIndex, fieldVector, fieldConverters);
    }

    private Object convertArray(
            int rowIndex, Converter converter, FieldVector fieldVector, ArrayType arrayType) {
        SqlType elementType = arrayType.getElementType().getSqlType();
        Map<String, Function> fieldConverters = new HashMap<>();
        fieldConverters.put(Converter.ARRAY_KEY, genericsConvert(elementType));
        return converter.convert(rowIndex, fieldVector, fieldConverters);
    }

    private Object convertRow(
            int rowIndex, Converter converter, FieldVector fieldVector, SeaTunnelRowType rowType) {
        String[] fieldNames = rowType.getFieldNames();
        List<SeaTunnelDataType<?>> fieldTypes = rowType.getChildren();
        Map<String, Function> fieldConverters = new HashMap<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldConverters.put(fieldNames[i], genericsConvert(fieldTypes.get(i).getSqlType()));
        }
        return converter.convert(rowIndex, fieldVector, fieldConverters);
    }

    private Function<Object, Object> genericsConvert(SqlType sqlType) {
        return value -> convertSeatunnelRowValue(sqlType, null, value);
    }

    @Override
    public void close() {
        try {
            if (root != null) {
                root.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to close arrow stream reader.", e);
        }
    }
}
