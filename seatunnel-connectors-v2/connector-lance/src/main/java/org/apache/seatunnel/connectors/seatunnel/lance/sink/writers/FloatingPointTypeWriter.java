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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Writer for FloatingPoint type. */
public class FloatingPointTypeWriter extends BaseTypeWriter {
    @Override
    public void writeToVector(
            FieldVector vector,
            ArrowType arrowType,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
        Number numValue = (Number) value;
        if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
            ((Float4Vector) vector).setSafe(rowIndex, numValue.floatValue());
        } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
            ((Float8Vector) vector).setSafe(rowIndex, numValue.doubleValue());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported FloatingPoint precision: " + fpType.getPrecision());
        }
    }

    @Override
    public void writeToListWriter(
            UnionListWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
        Number numValue = (Number) value;
        if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
            writer.writeFloat4(numValue.floatValue());
        } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
            writer.writeFloat8(numValue.doubleValue());
        }
    }

    @Override
    public void writeToMapKey(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        writeMapFloat(writer.key(), arrowType, value);
    }

    @Override
    public void writeToMapValue(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        writeMapFloat(writer.value(), arrowType, value);
    }

    private void writeMapFloat(UnionMapWriter writer, ArrowType arrowType, Object value) {
        ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
        Number numValue = (Number) value;
        if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
            writer.float4().writeFloat4(numValue.floatValue());
        } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
            writer.float8().writeFloat8(numValue.doubleValue());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported FloatingPoint precision: " + fpType.getPrecision());
        }
    }

    @Override
    public void writeNullToMapValue(UnionMapWriter writer, ArrowType arrowType) {
        ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
        if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
            writer.value().float4().writeNull();
        } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
            writer.value().float8().writeNull();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported FloatingPoint precision: " + fpType.getPrecision());
        }
    }
}
