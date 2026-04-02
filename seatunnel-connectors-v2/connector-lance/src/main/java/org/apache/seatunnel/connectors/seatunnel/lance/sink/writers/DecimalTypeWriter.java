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

package org.apache.seatunnel.connectors.seatunnel.lance.sink.writers;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;

/** Writer for Decimal type. */
public class DecimalTypeWriter extends BaseTypeWriter {
    @Override
    public void writeToVector(
            FieldVector vector,
            ArrowType arrowType,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        java.math.BigDecimal decimalValue = convertToBigDecimal(value, arrowType);
        ((DecimalVector) vector).setSafe(rowIndex, decimalValue);
    }

    private java.math.BigDecimal convertToBigDecimal(Object value, ArrowType arrowType) {
        java.math.BigDecimal decimalValue;
        if (value instanceof java.math.BigDecimal) {
            decimalValue = (java.math.BigDecimal) value;
        } else if (value instanceof Number) {
            decimalValue = java.math.BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            decimalValue = new java.math.BigDecimal(value.toString());
        }

        // Adjust scale to match Arrow Schema definition
        if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
            int requiredScale = decimalType.getScale();
            if (decimalValue.scale() != requiredScale) {
                decimalValue = decimalValue.setScale(requiredScale, RoundingMode.HALF_UP);
            }
        }

        return decimalValue;
    }

    @Override
    public void writeToListWriter(
            UnionListWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        java.math.BigDecimal decimalValue = convertToBigDecimal(value, arrowType);
        byte[] bytes = decimalValue.toString().getBytes(StandardCharsets.UTF_8);
        ArrowBuf buffer = createArrowBuf(bytes, allocator);
        try {
            writer.writeVarChar(0, bytes.length, buffer);
        } finally {
            buffer.close();
        }
    }

    @Override
    public void writeToMapKey(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        writeToListWriter(writer, arrowType, value, allocator);
    }

    @Override
    public void writeToMapValue(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        writeToListWriter(writer, arrowType, value, allocator);
    }
}
