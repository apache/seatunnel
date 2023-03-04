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

import lombok.Getter;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class MultipleRowType
        implements SeaTunnelDataType<SeaTunnelRow>, Iterable<Map.Entry<String, SeaTunnelRowType>> {
    private final Map<String, SeaTunnelRowType> rowTypeMap;
    @Getter private String[] tableIds;

    public MultipleRowType(String[] tableIds, SeaTunnelRowType[] rowTypes) {
        Map<String, SeaTunnelRowType> rowTypeMap = new LinkedHashMap<>();
        for (int i = 0; i < tableIds.length; i++) {
            rowTypeMap.put(tableIds[i], rowTypes[i]);
        }
        this.tableIds = tableIds;
        this.rowTypeMap = rowTypeMap;
    }

    public MultipleRowType(Map<String, SeaTunnelRowType> rowTypeMap) {
        this.tableIds = rowTypeMap.keySet().toArray(new String[0]);
        this.rowTypeMap = rowTypeMap;
    }

    public SeaTunnelRowType getRowType(String tableId) {
        return rowTypeMap.get(tableId);
    }

    @Override
    public Class<SeaTunnelRow> getTypeClass() {
        return SeaTunnelRow.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.MULTIPLE_ROW;
    }

    @Override
    public Iterator<Map.Entry<String, SeaTunnelRowType>> iterator() {
        return rowTypeMap.entrySet().iterator();
    }
}
