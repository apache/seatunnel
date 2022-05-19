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

package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.serialization.RowSerialization;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public final class InternalRowSerialization implements RowSerialization<InternalRow> {

    private final StructType sparkSchema;

    public InternalRowSerialization(StructType sparkSchema) {
        this.sparkSchema = sparkSchema;
    }

    @Override
    public InternalRow serialize(SeaTunnelRow seaTunnelRow) throws IOException {
        SpecificInternalRow sparkRow = new SpecificInternalRow(sparkSchema);
        Object[] fields = seaTunnelRow.getFields();
        for (int i = 0; i < fields.length; i++) {
            setField(fields[i], i,  sparkRow);
        }
        return sparkRow;
    }

    @Override
    public SeaTunnelRow deserialize(InternalRow engineRow) throws IOException {
        Object[] fields = new Object[engineRow.numFields()];
        for (int i = 0; i < engineRow.numFields(); i++) {
            fields[i] = engineRow.get(i, sparkSchema.apply(i).dataType());
        }
        return new SeaTunnelRow(fields);
    }

    private void setField(Object field, int index, InternalRow sparkRow) {
        if (field == null) {
            sparkRow.setNullAt(index);
        } else if (field instanceof Byte) {
            sparkRow.setByte(index, (byte) field);
        } else if (field instanceof Short) {
            sparkRow.setShort(index, (short) field);
        } else if (field instanceof Integer) {
            sparkRow.setInt(index, (int) field);
        } else if (field instanceof Long) {
            sparkRow.setLong(index, (long) field);
        } else if (field instanceof Boolean) {
            sparkRow.setBoolean(index, (boolean) field);
        } else if (field instanceof Double) {
            sparkRow.setDouble(index, (double) field);
        } else if (field instanceof Float) {
            sparkRow.setFloat(index, (float) field);
        } else {
            throw new RuntimeException(String.format("Unsupported data type: %s", field.getClass()));
        }
    }
}
