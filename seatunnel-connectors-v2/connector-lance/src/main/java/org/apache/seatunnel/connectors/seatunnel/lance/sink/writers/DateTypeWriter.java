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
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.time.LocalDate;
import java.time.ZoneId;

/** Writer for Date type. */
public class DateTypeWriter extends BaseTypeWriter {
    @Override
    public void writeToVector(
            FieldVector vector,
            ArrowType arrowType,
            Object value,
            int rowIndex,
            BufferAllocator allocator) {
        ArrowType.Date dateType = (ArrowType.Date) arrowType;
        if (dateType.getUnit() == DateUnit.DAY) {
            long epochDay = convertToEpochDay(value);
            ((DateDayVector) vector).setSafe(rowIndex, (int) epochDay);
        } else if (dateType.getUnit() == DateUnit.MILLISECOND) {
            long epochMilli = convertToEpochMilli(value);
            ((DateMilliVector) vector).setSafe(rowIndex, epochMilli);
        } else {
            throw new IllegalArgumentException("Unsupported Date unit: " + dateType.getUnit());
        }
    }

    private long convertToEpochDay(Object value) {
        if (value instanceof LocalDate) {
            return ((LocalDate) value).toEpochDay();
        } else if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate().toEpochDay();
        } else {
            return LocalDate.parse(value.toString()).toEpochDay();
        }
    }

    private long convertToEpochMilli(Object value) {
        if (value instanceof LocalDate) {
            return ((LocalDate) value)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        } else if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).getTime();
        } else if (value instanceof java.util.Date) {
            return ((java.util.Date) value).getTime();
        } else {
            throw new IllegalArgumentException("Unsupported date value type: " + value.getClass());
        }
    }

    @Override
    public void writeToListWriter(
            UnionListWriter writer, ArrowType arrowType, Object value, BufferAllocator allocator) {
        ArrowType.Date dateType = (ArrowType.Date) arrowType;
        if (dateType.getUnit() == DateUnit.DAY) {
            writer.writeInt((int) convertToEpochDay(value));
        } else {
            writer.writeBigInt(convertToEpochMilli(value));
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
