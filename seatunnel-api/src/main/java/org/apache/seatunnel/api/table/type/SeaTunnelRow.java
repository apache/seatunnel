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

package org.apache.seatunnel.api.table.type;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** SeaTunnel row type. */
public final class SeaTunnelRow implements Serializable {
    private static final long serialVersionUID = -1L;
    /** Table identifier. */
    private String tableId = "";
    /** The kind of change that a row describes in a changelog. */
    private RowKind rowKind = RowKind.INSERT;
    /** The array to store the actual internal format values. */
    private final Object[] fields;

    private Map<String, Object> options;

    private volatile int size;

    public SeaTunnelRow(int arity) {
        this.fields = new Object[arity];
    }

    public SeaTunnelRow(Object[] fields) {
        this.fields = fields;
    }

    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    public int getArity() {
        return fields.length;
    }

    public String getTableId() {
        return tableId;
    }

    public RowKind getRowKind() {
        return this.rowKind;
    }

    public Map<String, Object> getOptions() {
        if (options == null) {
            options = new HashMap<>();
        }
        return options;
    }

    public Object[] getFields() {
        return fields;
    }

    public Object getField(int pos) {
        return this.fields[pos];
    }

    public SeaTunnelRow copy() {
        Object[] newFields = new Object[this.getArity()];
        System.arraycopy(this.getFields(), 0, newFields, 0, newFields.length);
        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(this.getRowKind());
        newRow.setTableId(this.getTableId());
        return newRow;
    }

    public SeaTunnelRow copy(int[] indexMapping) {
        Object[] newFields = new Object[indexMapping.length];
        for (int i = 0; i < indexMapping.length; i++) {
            newFields[i] = this.fields[indexMapping[i]];
        }
        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(this.getRowKind());
        newRow.setTableId(this.getTableId());
        return newRow;
    }

    public boolean isNullAt(int pos) {
        return this.fields[pos] == null;
    }

    public int getBytesSize(SeaTunnelRowType rowType) {
        if (size == 0) {
            int s = 0;
            for (int i = 0; i < fields.length; i++) {
                s += getBytesForValue(fields[i], rowType.getFieldType(i));
            }
            size = s;
        }
        return size;
    }

    /** faster version of {@link #getBytesSize(SeaTunnelRowType)}. */
    private int getBytesForValue(Object v, SeaTunnelDataType<?> dataType) {
        if (v == null) {
            return 0;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case STRING:
                return ((String) v).length();
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
            case FLOAT:
                return 4;
            case BIGINT:
            case DOUBLE:
                return 8;
            case DECIMAL:
                return 36;
            case NULL:
                return 0;
            case BYTES:
                return ((byte[]) v).length;
            case DATE:
                return 24;
            case TIME:
                return 12;
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return 48;
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
            case BINARY_VECTOR:
                return ((ByteBuffer) v).capacity();
            case SPARSE_FLOAT_VECTOR:
                return ((Map<?, ?>) v).entrySet().size() * 8;
            case ARRAY:
                SeaTunnelDataType elementType = ((ArrayType) dataType).getElementType();
                if (elementType instanceof DecimalType) {
                    return ((Object[]) v).length * 36;
                }
                if (elementType instanceof LocalTimeType) {
                    SqlType eleSqlType = elementType.getSqlType();
                    switch (eleSqlType) {
                        case DATE:
                            return ((Object[]) v).length * 24;
                        case TIME:
                            return ((Object[]) v).length * 12;
                        case TIMESTAMP:
                        case TIMESTAMP_TZ:
                            return ((Object[]) v).length * 48;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported type in LocalTimeArrayType: " + eleSqlType);
                    }
                }

                return getBytesForArray(v, ((ArrayType) dataType).getElementType());
            case MAP:
                int size = 0;
                MapType<?, ?> mapType = ((MapType<?, ?>) dataType);
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                    size +=
                            getBytesForValue(entry.getKey(), mapType.getKeyType())
                                    + getBytesForValue(entry.getValue(), mapType.getValueType());
                }
                return size;
            case ROW:
                int rowSize = 0;
                SeaTunnelRowType rowType = ((SeaTunnelRowType) dataType);
                SeaTunnelDataType<?>[] types = rowType.getFieldTypes();
                SeaTunnelRow row = (SeaTunnelRow) v;
                for (int i = 0; i < types.length; i++) {
                    rowSize += getBytesForValue(row.fields[i], types[i]);
                }
                return rowSize;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + sqlType);
        }
    }

    private int getBytesForArray(Object v, SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                int s = 0;
                for (String i : ((String[]) v)) {
                    s += i == null ? 0 : i.length();
                }
                return s;
            case BOOLEAN:
                return getArrayNotNullSize((Boolean[]) v);
            case TINYINT:
                return getArrayNotNullSize((Byte[]) v);
            case SMALLINT:
                return getArrayNotNullSize((Short[]) v) * 2;
            case INT:
                return getArrayNotNullSize((Integer[]) v) * 4;
            case FLOAT:
                return getArrayNotNullSize((Float[]) v) * 4;
            case BIGINT:
                return getArrayNotNullSize((Long[]) v) * 8;
            case DOUBLE:
                return getArrayNotNullSize((Double[]) v) * 8;
            case MAP:
                return getArrayMapNotNullSize(v);
            case NULL:
            default:
                return 0;
        }
    }

    private int getArrayNotNullSize(Object[] values) {
        int c = 0;
        for (Object value : values) {
            if (value != null) {
                c++;
            }
        }
        return c;
    }

    private int getArrayMapNotNullSize(Object v) {
        int size = 0;
        if (Objects.nonNull(v)) {
            for (Map o : (Map[]) v) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) o).entrySet()) {
                    size += getBytesForValue(entry.getKey()) + getBytesForValue(entry.getValue());
                }
            }
        }

        return size;
    }

    public int getBytesSize() {
        if (size == 0) {
            int s = 0;
            for (Object field : fields) {
                s += getBytesForValue(field);
            }
            size = s;
        }
        return size;
    }

    private int getBytesForValue(Object v) {
        if (v == null) {
            return 0;
        }
        String clazz = v.getClass().getSimpleName();
        switch (clazz) {
            case "String":
                return ((String) v).length();
            case "Boolean":
            case "Byte":
                return 1;
            case "Short":
                return 2;
            case "Integer":
            case "Float":
                return 4;
            case "Long":
            case "Double":
                return 8;
            case "BigDecimal":
                return 36;
            case "byte[]":
                return ((byte[]) v).length;
            case "LocalDate":
                return 24;
            case "LocalTime":
                return 12;
            case "LocalDateTime":
            case "OffsetDateTime":
                return 48;
            case "String[]":
                return getBytesForArray(v, BasicType.STRING_TYPE);
            case "Boolean[]":
                return getBytesForArray(v, BasicType.BOOLEAN_TYPE);
            case "Byte[]":
                return getBytesForArray(v, BasicType.BYTE_TYPE);
            case "Short[]":
                return getBytesForArray(v, BasicType.SHORT_TYPE);
            case "Integer[]":
                return getBytesForArray(v, BasicType.INT_TYPE);
            case "Long[]":
                return getBytesForArray(v, BasicType.LONG_TYPE);
            case "Float[]":
                return getBytesForArray(v, BasicType.FLOAT_TYPE);
            case "Double[]":
                return getBytesForArray(v, BasicType.DOUBLE_TYPE);
            case "Map[]":
                return getBytesForArray(
                        v, new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE));
            case "HashMap":
            case "LinkedHashMap":
                int size = 0;
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                    size += getBytesForValue(entry.getKey()) + getBytesForValue(entry.getValue());
                }
                return size;
            case "HeapByteBuffer":
            case "ByteBuffer":
                return ((ByteBuffer) v).capacity();
            case "SeaTunnelRow":
                int rowSize = 0;
                SeaTunnelRow row = (SeaTunnelRow) v;
                for (int i = 0; i < row.fields.length; i++) {
                    rowSize += getBytesForValue(row.fields[i]);
                }
                return rowSize;
            default:
                if (v instanceof Map) {
                    int mapSize = 0;
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                        mapSize +=
                                getBytesForValue(entry.getKey())
                                        + getBytesForValue(entry.getValue());
                    }
                    return mapSize;
                }
                throw new UnsupportedOperationException("Unsupported type: " + clazz);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SeaTunnelRow)) {
            return false;
        }
        SeaTunnelRow that = (SeaTunnelRow) o;
        return Objects.equals(tableId, that.tableId)
                && rowKind == that.rowKind
                && Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableId, rowKind);
        result = 31 * result + Arrays.deepHashCode(fields);
        return result;
    }

    @Override
    public String toString() {
        return "SeaTunnelRow{"
                + "tableId="
                + tableId
                + ", kind="
                + rowKind.shortString()
                + ", fields="
                + Arrays.toString(fields)
                + '}';
    }
}
