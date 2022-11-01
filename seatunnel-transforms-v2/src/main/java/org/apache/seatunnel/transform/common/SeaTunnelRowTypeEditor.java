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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SeaTunnelRowTypeEditor implements Serializable {
    private final LinkedList<Field> fields;

    public SeaTunnelRowTypeEditor(SeaTunnelRowType rowType) {
        log.info("Input row type: {}", rowType);
        this.fields = IntStream.range(0, rowType.getTotalFields())
            .boxed()
            .map(index -> new Field(rowType.getFieldName(index), rowType.getFieldType(index)))
            .collect(Collectors.toCollection(LinkedList::new));
    }

    public SeaTunnelRowTypeEditor remove(String... fieldNames) {
        for (String fieldName : fieldNames) {
            int index = fields.indexOf(Field.name(fieldName));
            fields.remove(index);
        }
        return this;
    }

    public SeaTunnelRowTypeEditor addLast(String fieldName, SeaTunnelDataType dataType) {
        fields.addLast(new Field(fieldName, dataType));
        return this;
    }

    public SeaTunnelRowTypeEditor addFirst(String fieldName, SeaTunnelDataType dataType) {
        fields.addFirst(new Field(fieldName, dataType));
        return this;
    }

    public SeaTunnelRowTypeEditor add(int index, String fieldName, SeaTunnelDataType dataType) {
        fields.add(index, new Field(fieldName, dataType));
        return this;
    }

    public SeaTunnelRowTypeEditor update(String fieldName, SeaTunnelDataType dataType) {
        int index = fields.indexOf(Field.name(fieldName));
        if (index == -1) {
            throw new IllegalArgumentException("");
        }

        Field field = fields.get(index);
        field.setDataType(dataType);
        return this;
    }

    public SeaTunnelRowTypeEditor moveBefore(String srcFieldName, String destFieldName) {
        int srcFieldIndex = fields.indexOf(Field.name(srcFieldName));
        int destFieldIndex = fields.indexOf(Field.name(destFieldName));

        Field srcField = fields.get(srcFieldIndex);
        fields.remove(srcField);
        fields.add(destFieldIndex, srcField);
        return this;
    }

    public SeaTunnelRowTypeEditor moveAfter(String srcFieldName, String destFieldName) {
        int srcFieldIndex = fields.indexOf(Field.name(srcFieldName));
        int destFieldIndex = fields.indexOf(Field.name(destFieldName));

        Field srcField = fields.get(srcFieldIndex);
        fields.remove(srcField);
        fields.add(destFieldIndex + 1, srcField);
        return this;
    }

    public SeaTunnelRowType build() {
        String[] fieldNames = new String[fields.size()];
        SeaTunnelDataType[] fieldDataTypes = new SeaTunnelDataType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            fieldNames[i] = field.getName();
            fieldDataTypes[i] = field.getDataType();
        }

        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldDataTypes);
        log.info("Output row type: {}", rowType);

        return rowType;
    }

    @AllArgsConstructor
    @EqualsAndHashCode(of = "name")
    @ToString
    private static class Field implements Serializable {
        @Getter
        private String name;
        @Setter
        @Getter
        private SeaTunnelDataType dataType;

        static Field name(String name) {
            return new Field(name, null);
        }
    }
}
