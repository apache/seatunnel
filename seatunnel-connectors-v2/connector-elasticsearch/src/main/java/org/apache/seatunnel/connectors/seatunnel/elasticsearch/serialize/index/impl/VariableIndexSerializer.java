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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.impl;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.IndexSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * index include variable
 */
public class VariableIndexSerializer implements IndexSerializer {

    private final String index;
    private final Map<String, Integer> fieldIndexMap;

    private final String nullDefault = "null";

    public VariableIndexSerializer(SeaTunnelRowType seaTunnelRowType, String index, List<String> fieldNames) {
        this.index = index;
        String[] rowFieldNames = seaTunnelRowType.getFieldNames();
        fieldIndexMap = new HashMap<>(rowFieldNames.length);
        for (int i = 0; i < rowFieldNames.length; i++) {
            if (fieldNames.contains(rowFieldNames[i])) {
                fieldIndexMap.put(rowFieldNames[i], i);
            }
        }
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        String indexName = this.index;
        for (Map.Entry<String, Integer> fieldIndexEntry : fieldIndexMap.entrySet()) {
            String fieldName = fieldIndexEntry.getKey();
            int fieldIndex = fieldIndexEntry.getValue();
            String value = getValue(fieldIndex, row);
            indexName = indexName.replace(String.format("${%s}", fieldName), value);
        }
        return indexName.toLowerCase();
    }

    private String getValue(int fieldIndex, SeaTunnelRow row) {
        Object valueObj = row.getField(fieldIndex);
        if (valueObj == null) {
            return nullDefault;
        } else {
            return valueObj.toString();
        }
    }
}
