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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import lombok.NonNull;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

public class IcebergRecordProjection implements Record {

    private final Record record;
    private final Types.StructType structType;
    private final Types.StructType projectStructType;
    private final Map<Integer, Integer> posMapping;

    public IcebergRecordProjection(@NonNull Record record,
                                   @NonNull Types.StructType structType,
                                   @NonNull Types.StructType projectStructType) {
        Map<Integer, Integer> posMapping = new HashMap<>();
        for (int projectPos = 0, len = projectStructType.fields().size();
             projectPos < len; projectPos++) {
            Types.NestedField projectField = projectStructType.fields().get(projectPos);

            Types.NestedField field = structType.field(projectField.fieldId());
            int fieldPos = structType.fields().indexOf(field);
            posMapping.put(projectPos, fieldPos);
        }

        this.record = record;
        this.structType = structType;
        this.projectStructType = projectStructType;
        this.posMapping = posMapping;
    }

    @Override
    public Types.StructType struct() {
        return projectStructType;
    }

    @Override
    public Object getField(String name) {
        return record.getField(name);
    }

    @Override
    public void setField(String name, Object value) {
        record.setField(name, value);
    }

    @Override
    public Object get(int pos) {
        return record.get(posMapping.get(pos));
    }

    @Override
    public Record copy() {
        return new IcebergRecordProjection(record.copy(), structType, projectStructType);
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        return new IcebergRecordProjection(record.copy(overwriteValues), structType, projectStructType);
    }

    @Override
    public int size() {
        return projectStructType.fields().size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return record.get(posMapping.get(pos), javaClass);
    }

    @Override
    public <T> void set(int pos, T value) {
        record.set(posMapping.get(pos), value);
    }
}
