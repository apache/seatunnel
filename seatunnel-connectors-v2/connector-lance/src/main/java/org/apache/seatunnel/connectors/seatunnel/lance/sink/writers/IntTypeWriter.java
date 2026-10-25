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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Writer for Int type. */
public class IntTypeWriter extends BaseTypeWriter {
    @Override
    public void writeToVector(
            FieldVector vector,
            ArrowType arrowType,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        ArrowType.Int intType = (ArrowType.Int) arrowType;
        int bitWidth = intType.getBitWidth();
        Number numValue = (Number) value;
        switch (bitWidth) {
            case 8:
                ((TinyIntVector) vector).setSafe(rowIndex, numValue.byteValue());
                break;
            case 16:
                ((SmallIntVector) vector).setSafe(rowIndex, numValue.shortValue());
                break;
            case 32:
                ((IntVector) vector).setSafe(rowIndex, numValue.intValue());
                break;
            case 64:
                ((BigIntVector) vector).setSafe(rowIndex, numValue.longValue());
                break;
            default:
                throw new IllegalArgumentException("Unsupported Int bit width: " + bitWidth);
        }
    }

    @Override
    public void writeToListWriter(
            UnionListWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        ArrowType.Int intType = (ArrowType.Int) arrowType;
        int bitWidth = intType.getBitWidth();
        Number numValue = (Number) value;
        switch (bitWidth) {
            case 8:
                writer.writeTinyInt(numValue.byteValue());
                break;
            case 16:
                writer.writeSmallInt(numValue.shortValue());
                break;
            case 32:
                writer.writeInt(numValue.intValue());
                break;
            case 64:
                writer.writeBigInt(numValue.longValue());
                break;
        }
    }

    @Override
    public void writeToMapKey(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        ArrowType.Int intType = (ArrowType.Int) arrowType;
        int bitWidth = intType.getBitWidth();
        Number numValue = (Number) value;
        switch (bitWidth) {
            case 8:
                writer.key().writeTinyInt(numValue.byteValue());
                break;
            case 16:
                writer.key().writeSmallInt(numValue.shortValue());
                break;
            case 32:
                writer.key().writeInt(numValue.intValue());
                break;
            case 64:
                writer.key().writeBigInt(numValue.longValue());
                break;
        }
    }

    @Override
    public void writeToMapValue(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        writeToListWriter(writer, arrowType, value, allocator);
    }
}
