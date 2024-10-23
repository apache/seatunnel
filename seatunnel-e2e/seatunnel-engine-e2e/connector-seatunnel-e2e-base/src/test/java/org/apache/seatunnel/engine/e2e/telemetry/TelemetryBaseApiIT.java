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

package org.apache.seatunnel.engine.e2e.telemetry;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

public class TelemetryBaseApiIT extends AbstractTelemetryBaseIT {

    private static HazelcastInstanceImpl hazelcastInstance;

    private static String testClusterName = "TelemetryApiIT";

    private void createBaseTestNode(SeaTunnelConfig[] seaTunnelConfigs) {
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfigs[0]);
    }

    @Override
    public void open(SeaTunnelConfig... seaTunnelConfigs) {
        createBaseTestNode(seaTunnelConfigs);
    }

    @Override
    public void close() throws Exception {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Override
    public HazelcastInstanceImpl getHazelcastInstance() throws Exception {
        return hazelcastInstance;
    }

    @Override
    public int getNodeCount() {
        return 1;
    }

    @Override
    public String getClusterName() {
        return TestUtils.getClusterName(testClusterName);
    }
}
