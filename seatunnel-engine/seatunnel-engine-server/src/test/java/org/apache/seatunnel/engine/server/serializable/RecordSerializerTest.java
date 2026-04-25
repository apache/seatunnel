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

package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;

class RecordSerializerTest {
    @Test
    void testSerializeSeaTunnelRowWithArityGreaterThanSignedByteMaxValue() {
        SeaTunnelRow row = new SeaTunnelRow(Byte.MAX_VALUE + 1);
        for (int i = 0; i < row.getArity(); i++) {
            row.setField(i, "field-" + i);
        }
        Record<SeaTunnelRow> record = new Record<>(row);
        HazelcastInstanceImpl instance = createHazelcastInstance();
        try {
            Data data = instance.getSerializationService().toData(record);
            Assertions.assertEquals(TypeId.RECORD, data.getType());
            Record<?> restored = instance.getSerializationService().toObject(data);

            Assertions.assertInstanceOf(SeaTunnelRow.class, restored.getData());
            SeaTunnelRow restoredRow = (SeaTunnelRow) restored.getData();
            Assertions.assertEquals(row.getArity(), restoredRow.getArity());
            Assertions.assertArrayEquals(row.getFields(), restoredRow.getFields());
        } finally {
            instance.shutdown();
        }
    }

    private HazelcastInstanceImpl createHazelcastInstance() {
        String clusterName =
                TestUtils.getClusterName(
                        "RecordSerializerTest_testSerializeSeaTunnelRowWithArityGreaterThanSignedByteMaxValue");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        Config hazelcastConfig = Config.loadFromString(buildHazelcastConfig(clusterName));
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        return SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    private String buildHazelcastConfig(String clusterName) {
        return "hazelcast:\n"
                + "  cluster-name: "
                + clusterName
                + "\n"
                + "  network:\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: false\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      auto-detection:\n"
                + "        enabled: false\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: 5801\n";
    }
}
