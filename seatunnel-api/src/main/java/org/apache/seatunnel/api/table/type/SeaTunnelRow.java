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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * SeaTunnel row type.
 */
public final class SeaTunnelRow implements Serializable {
    private static final long serialVersionUID = -1L;
    private final int tableId;
    // todo: add row kind
    private final Object[] fields;
    private final Map<String, Object> fieldMap;

    public SeaTunnelRow(Object[] fields) {
        this(fields, null);

    }

    public SeaTunnelRow(Object[] fields, Map<String, Object> fieldMap) {
        this(fields, fieldMap, -1);
    }

    public SeaTunnelRow(Object[] fields, Map<String, Object> fieldMap, int tableId) {
        this.fields = fields;
        this.fieldMap = fieldMap;
        this.tableId = tableId;
    }

    public int getTableId() {
        return tableId;
    }

    public Object[] getFields() {
        return fields;
    }

    public Map<String, Object> getFieldMap() {
        return fieldMap;
    }

    @Override
    public String toString() {
        return "SeaTunnelRow{" +
            "tableId=" + tableId +
            ", fields=" + Arrays.toString(fields) +
            ", fieldMap=" + fieldMap +
            '}';
    }
}
