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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Writer for Binary type. */
public class BinaryTypeWriter extends BaseTypeWriter {
    @Override
    public void writeToVector(
            FieldVector vector,
            ArrowType arrowType,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        byte[] bytes = getBytes(value);
        ((VarBinaryVector) vector).setSafe(rowIndex, bytes);
    }

    @Override
    public void writeToListWriter(
            UnionListWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        byte[] bytes = getBytes(value);
        ArrowBuf buffer = createArrowBuf(bytes, allocator);
        try {
            writer.writeVarBinary(0, bytes.length, buffer);
        } finally {
            buffer.close();
        }
    }

    @Override
    public void writeToMapKey(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        // Binary type is typically not used as map key, but handle it anyway
        writeToListWriter(writer, arrowType, value, allocator);
    }

    @Override
    public void writeToMapValue(
            UnionMapWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        writeToListWriter(writer, arrowType, value, allocator);
    }
}
