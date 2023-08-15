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

import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

public class SeaTunnelRowType implements CompositeType<SeaTunnelRow> {
    private static final long serialVersionUID = 2L;

    /** The field name of the {@link SeaTunnelRow}. */
    private final String[] fieldNames;
    /** The type of the field. */
    private final SeaTunnelDataType<?>[] fieldTypes;

    public SeaTunnelRowType(String[] fieldNames, SeaTunnelDataType<?>[] fieldTypes) {
        checkArgument(
                fieldNames.length == fieldTypes.length,
                "The number of field names must be the same as the number of field types.");
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public Class<SeaTunnelRow> getTypeClass() {
        return SeaTunnelRow.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.ROW;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public SeaTunnelDataType<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public List<SeaTunnelDataType<?>> getChildren() {
        return Arrays.asList(fieldTypes);
    }

    public int getTotalFields() {
        return fieldTypes.length;
    }

    public String getFieldName(int index) {
        return fieldNames[index];
    }

    public SeaTunnelDataType<?> getFieldType(int index) {
        return fieldTypes[index];
    }

    public int indexOf(String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("can't find field [%s]", fieldName));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SeaTunnelRowType)) {
            return false;
        }
        SeaTunnelRowType that = (SeaTunnelRowType) obj;
        return Arrays.equals(fieldNames, that.fieldNames)
                && Arrays.equals(fieldTypes, that.fieldTypes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(fieldNames);
        result = 31 * result + Arrays.hashCode(fieldTypes);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ROW<");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(fieldNames[i]).append(" ").append(fieldTypes[i]);
        }
        return builder.append(">").toString();
    }
}
