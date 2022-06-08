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

package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer<String, String> {

    private final String topic;
    private final SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;

    public DefaultSeaTunnelRowSerializer(String topic, SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        this.topic = topic;
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    public ProducerRecord<String, String> serializeRow(SeaTunnelRow row) {
        Map<Object, Object> map = new HashMap<>(Common.COLLECTION_SIZE);
        String[] fieldNames = seaTunnelRowTypeInfo.getFieldNames();
        Object[] fields = row.getFields();
        for (int i = 0; i < fieldNames.length; i++) {
            map.put(fieldNames[i], fields[i]);
        }
        return new ProducerRecord<>(topic, null, JsonUtils.toJsonString(map));
    }
}
