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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.format.PulsarCanalDecorator;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.Getter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PulsarCanalDecoratorTest {
    private static final String json =
            "{"
                    + "  \"id\": 3,\n"
                    + "  \"message\": \"[{\\\"data\\\":[{\\\"isKey\\\":\\\"1\\\",\\\"isNull\\\":\\\"0\\\",\\\"index\\\":\\\"0\\\",\\\"mysqlType\\\":\\\"INTEGER\\\",\\\"columnName\\\":\\\"id\\\",\\\"columnValue\\\":\\\"109\\\",\\\"updated\\\":\\\"0\\\"},{\\\"isKey\\\":\\\"0\\\",\\\"isNull\\\":\\\"0\\\",\\\"index\\\":\\\"1\\\",\\\"mysqlType\\\":\\\"VARCHAR(255)\\\",\\\"columnName\\\":\\\"name\\\",\\\"columnValue\\\":\\\"spare tire\\\",\\\"updated\\\":\\\"0\\\"},{\\\"isKey\\\":\\\"0\\\",\\\"isNull\\\":\\\"0\\\",\\\"index\\\":\\\"2\\\",\\\"mysqlType\\\":\\\"VARCHAR(512)\\\",\\\"columnName\\\":\\\"description\\\",\\\"columnValue\\\":\\\"24 inch spare tire\\\",\\\"updated\\\":\\\"0\\\"},{\\\"isKey\\\":\\\"0\\\",\\\"isNull\\\":\\\"0\\\",\\\"index\\\":\\\"3\\\",\\\"mysqlType\\\":\\\"VARCHAR(512)\\\",\\\"columnName\\\":\\\"weight\\\",\\\"columnValue\\\":\\\"22.2\\\",\\\"updated\\\":\\\"0\\\"}],\\\"database\\\":\\\"canal_17yaa8a\\\",\\\"es\\\":1680412018000,\\\"id\\\":3,\\\"isDdl\\\":false,\\\"mysqlType\\\":null,\\\"old\\\":null,\\\"sql\\\":\\\"\\\",\\\"sqlType\\\":null,\\\"table\\\":\\\"products\\\",\\\"ts\\\":1680412018293,\\\"type\\\":\\\"DELETE\\\"}]\",\n"
                    + "  \"timestamp\": \"2023-04-02 05:06:58\""
                    + "}";

    @Test
    void decoder() throws IOException {
        String[] fieldNames = new String[] {"id", "name", "description", "weight"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType[] {
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE
                };

        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fieldNames, dataTypes);

        CanalJsonDeserializationSchema canalJsonDeserializationSchema =
                CanalJsonDeserializationSchema.builder(seaTunnelRowType).build();
        PulsarCanalDecorator pulsarCanalDecorator =
                new PulsarCanalDecorator(canalJsonDeserializationSchema);

        SimpleCollector simpleCollector = new SimpleCollector();
        pulsarCanalDecorator.deserialize(json.getBytes(StandardCharsets.UTF_8), simpleCollector);
        Assertions.assertFalse(simpleCollector.getList().isEmpty());
        for (SeaTunnelRow seaTunnelRow : simpleCollector.list) {
            for (Object field : seaTunnelRow.getFields()) {
                Assertions.assertNotNull(field);
            }
        }
    }

    private static class SimpleCollector implements Collector<SeaTunnelRow> {
        @Getter private List<SeaTunnelRow> list = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            list.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }
}
