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
package org.apache.seatunnel.connectors.seatunnel.neo4j.internal;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.AsValue;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.MapValue;

import java.util.Map;

/**
 * This class includes the seatunnelRow and implements the neo4j.driver.internal.AsValue interface.
 * This class will be able to convert to neo4j.driver.Value quickly without any extra effort.
 */
public class SeaTunnelRowNeo4jValue implements AsValue {
    private final SeaTunnelRowType seaTunnelRowType;
    private final SeaTunnelRow seaTunnelRow;

    public SeaTunnelRowNeo4jValue(SeaTunnelRowType seaTunnelRowType, SeaTunnelRow seaTunnelRow) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.seaTunnelRow = seaTunnelRow;
    }

    @Override
    public Value asValue() {
        int length = seaTunnelRowType.getTotalFields();
        Map<String, Value> valueMap = Iterables.newHashMapWithSize(length);
        for (int i = 0; i < length; i++) {
            String name = seaTunnelRowType.getFieldName(i);
            Value value = Values.value(seaTunnelRow.getField(i));
            valueMap.put(name, value);
        }
        return new MapValue(valueMap);
    }
}
