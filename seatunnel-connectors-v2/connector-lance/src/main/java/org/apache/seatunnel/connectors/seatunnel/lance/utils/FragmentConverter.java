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

package org.apache.seatunnel.connectors.seatunnel.lance.utils;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.lance.sink.writers.TypeWriter;
import org.apache.seatunnel.connectors.seatunnel.lance.sink.writers.TypeWriterFactory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.WriteParams;

import java.util.List;

/** The converter for converting {@link Fragment} and {@link SeaTunnelRow} * */
public class FragmentConverter {

    private FragmentConverter() {}

    public static List<FragmentMetadata> reconvert(
            SeaTunnelRow seaTunnelRow,
            SeaTunnelRowType seaTunnelRowType,
            Schema schema,
            BufferAllocator allocator,
            String datasetPath) {

        List<FragmentMetadata> fragmentMetas;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            int rowIndex = 0;
            for (Field field : schema.getFields()) {
                FieldVector vector = root.getVector(field.getName());
                int fieldIndex = seaTunnelRowType.indexOf(field.getName());
                if (fieldIndex >= 0) {
                    Object fieldValue = seaTunnelRow.getField(fieldIndex);
                    setVectorValue(vector, field, fieldValue, rowIndex, allocator);
                }
            }
            root.setRowCount(1);
            fragmentMetas =
                    Fragment.create(
                            datasetPath,
                            allocator,
                            root,
                            new WriteParams.Builder()
                                    .withMaxRowsPerFile(Integer.MAX_VALUE)
                                    .build());
            return fragmentMetas;
        }
    }

    private static void setVectorValue(
            FieldVector vector,
            Field field,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        ArrowType arrowType = field.getType();
        if (value == null) {
            vector.setNull(rowIndex);
            return;
        }

        if (arrowType instanceof ArrowType.List) {
            writeListToVector((ListVector) vector, field, value, rowIndex, allocator);
        } else if (arrowType instanceof ArrowType.Map) {
            writeMapToVector((MapVector) vector, field, value, rowIndex, allocator);
        } else {
            TypeWriter writer = TypeWriterFactory.getWriter(arrowType);
            writer.writeToVector(vector, arrowType, value, rowIndex, allocator);
        }
    }

    private static void writeListToVector(
            ListVector listVector,
            Field field,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        if (!(value instanceof java.util.List)) {
            throw new IllegalArgumentException(
                    "List type requires List value, got: " + value.getClass());
        }

        UnionListWriter writer = listVector.getWriter();
        writer.setPosition(rowIndex);
        writer.startList();

        java.util.List<?> listValue = (java.util.List<?>) value;
        List<Field> children = field.getChildren();
        if (children.isEmpty()) {
            throw new IllegalArgumentException("List field must have a child field");
        }
        Field elementField = children.get(0);
        ArrowType elementType = elementField.getType();

        for (Object element : listValue) {
            writeListElement(writer, elementType, element, allocator);
        }

        writer.setValueCount(listValue.size());
        writer.endList();
    }

    private static void writeMapToVector(
            MapVector mapVector,
            Field field,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        if (!(value instanceof java.util.Map)) {
            throw new IllegalArgumentException(
                    "Map type requires Map value, got: " + value.getClass());
        }

        UnionMapWriter writer = mapVector.getWriter();
        writer.setPosition(rowIndex);
        writer.startMap();

        java.util.Map<?, ?> mapValue = (java.util.Map<?, ?>) value;
        List<Field> children = field.getChildren();
        if (children.size() < 2) {
            throw new IllegalArgumentException("Map field must have key and value child fields");
        }
        Field keyField = children.get(0);
        Field valueField = children.get(1);
        ArrowType keyType = keyField.getType();
        ArrowType valueType = valueField.getType();

        for (java.util.Map.Entry<?, ?> entry : mapValue.entrySet()) {
            writer.startEntry();
            writeMapKey(writer, keyType, entry.getKey(), allocator);
            writeMapValue(writer, valueType, entry.getValue(), allocator);
            writer.endEntry();
        }
        writer.endMap();
    }

    private static void writeListElement(
            UnionListWriter writer,
            ArrowType elementType,
            Object element,
            BufferAllocator allocator) {
        if (element == null) {
            writer.writeNull();
            return;
        }

        TypeWriter typeWriter = TypeWriterFactory.getWriter(elementType);
        typeWriter.writeToListWriter(writer, elementType, element, allocator);
    }

    private static void writeMapKey(
            UnionMapWriter writer, ArrowType keyType, Object key, BufferAllocator allocator) {
        if (key == null) {
            throw new IllegalArgumentException("Map key cannot be null");
        }

        TypeWriter typeWriter = TypeWriterFactory.getWriter(keyType);
        typeWriter.writeToMapKey(writer, keyType, key, allocator);
    }

    private static void writeMapValue(
            UnionMapWriter writer, ArrowType valueType, Object value, BufferAllocator allocator) {
        if (value == null) {
            writer.value().writeNull();
            return;
        }

        TypeWriter typeWriter = TypeWriterFactory.getWriter(valueType);
        typeWriter.writeToMapValue(writer, valueType, value, allocator);
    }
}
